import asyncio
import multiprocessing as mp
import numpy as np
import base64
import yaml
import asyncpg
import json
import logging
import cv2
import os
from confluent_kafka import Consumer
from queue import Empty, Full
from concurrent.futures import ThreadPoolExecutor

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s [%(name)s] %(message)s'
)

def load_config(path="config.yaml"):
    try:
        logger.info(f"Loading config from: {os.path.abspath(path)}")
        with open(path, "r") as f:
            return yaml.safe_load(f)
    except Exception as e:
        logger.exception(f"Failed to load config: {str(e)}")
        raise


def load_model():
    with open('./model/classification_classes_ILSVRC2012.txt', 'r') as f:
        classes = [line.strip().split(' ', 1)[1] if len(line.strip().split(' ', 1)) > 1 else '' for line in f]

    net = cv2.dnn.readNet(
        './model/mobilenet_deploy.prototxt',
        './model/mobilenet.caffemodel'
    )
    return net, classes


def real_model_inference(frame_bytes, net, classes):
    nparr = np.frombuffer(frame_bytes, np.uint8)
    img = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
    if img is None:
        logger.error(f"cv2.imdecode failed for scenario")
        return {
            "class": "noclass",
            "confidence": 0.0
        }

    blob = cv2.dnn.blobFromImage(
        img,
        0.017,
        (224, 224),
        (103.94, 116.78, 123.68),
        swapRB=False
    )

    net.setInput(blob)
    preds = net.forward()

    class_id = int(np.argmax(preds))
    confidence = preds[0][class_id]

    return {
        "class": classes[class_id],
        "confidence": float(confidence)
    }

def mock_model_inference(frame_bytes):
    import time
    time.sleep(0.1)
    return {
        "class": "cat",
    }

def inference_worker(input_queue, output_queue):
    logger = logging.getLogger(__name__)
    logger.info("[Inference] Started")
    net, classes = load_model()
    logger.info(f"Model loaded, {len(classes)} classes available")
    while True:
        frame_obj = input_queue.get()
        if frame_obj is None:
            logger.info("[Inference] Stopping")
            break

        scenario_id = frame_obj['scenario_id']
        frame_bytes = frame_obj['frame_bytes']
        try:
            result = real_model_inference(frame_bytes, net, classes)
            result['scenario_id'] = scenario_id

        except Exception as e:
            logger.error(f"Inference error: {str(e)}")
            result = { "class": "error", "confidence": 0.0, "scenario_id": scenario_id}

        logger.debug(f"Inference result: {result['class']} ({result['confidence']:.2f})")
        output_queue.put(result)

    logger.info("Inference worker stopped")

async def consume_kafka(input_queue, kafka_conf, executor):
    logger.info(f"Starting Kafka consumer for topic: {kafka_conf['topic']}")
    conf = {
        'bootstrap.servers': kafka_conf['bootstrap_servers'],
        'group.id': kafka_conf['group_id'],
        'auto.offset.reset': 'earliest'
    }
    consumer = Consumer(conf)
    consumer.subscribe([kafka_conf['topic']])

    loop = asyncio.get_running_loop()

    try:
        while True:
            msg = await loop.run_in_executor(executor, consumer.poll, 0.1)
            if msg is None:
                await asyncio.sleep(0.01)
                continue
            if msg.error():
                logger.error(f"Kafka error: {msg.error()}")
                continue

            try:
                message = json.loads(msg.value().decode('utf-8'))
                scenario_id = message.get('scenario_id', 'unknown')
                frame_data = base64.b64decode(message['data'])
                frame_obj = {'scenario_id': scenario_id, 'frame_bytes': frame_data}

                try:
                    input_queue.put_nowait(frame_obj)
                except Full:
                    logger.warning("Input queue full, dropping frame")

            except (json.JSONDecodeError, KeyError, UnicodeDecodeError) as e:
                logger.error(f"Error processing message: {str(e)}")
    except asyncio.CancelledError:
        logger.info("Kafka consumer cancelled")
    finally:
        consumer.close()
        logger.info("Kafka consumer closed")

async def db_writer(output_queue, pg_dsn, executor):
    logger.info("Starting DB writer")
    try:
        pool = await asyncpg.create_pool(dsn=pg_dsn)
        logger.info("Connected to database")
    except Exception as e:
        logger.error(f"Database connection failed: {str(e)}")
        return

    loop = asyncio.get_running_loop()

    try:
        while True:
            try:
                result = await loop.run_in_executor(executor, output_queue.get, False)

            except Empty:
                await asyncio.sleep(0.01)
                continue

            if result is None:
                logger.info("Received stop signal, exiting")
                break

            await process_single_record(pool, result)
    except asyncio.CancelledError:
        logger.info("DB writer cancelled")
    finally:
        await pool.close()
        logger.info("DB pool closed")

async def process_single_record(pool, result):
    try:
        async with pool.acquire() as conn:
            async with conn.transaction():
                scenario_id = result.get('scenario_id', 'unknown')
                image_id = await conn.fetchval(
                    "INSERT INTO images(scenario_id, class, confidence) VALUES($1, $2, $3) RETURNING id",
                    scenario_id,
                    result.get("class", "noclass"),
                    result.get("confidence", 0.0)
                )

                payload = json.dumps({
                    "scenario_id": scenario_id,
                    "class": result.get("class", "noclass"),
                    "confidence": result.get("confidence", 0.0)
                })

                await conn.execute(
                    "INSERT INTO outbox(aggregate_type, aggregate_id, event_type, payload) VALUES($1, $2, $3, $4)",
                    "image",
                    str(image_id),
                    "created",
                    payload
                )

                logger.info(f"Inserted classification: {result['class']} ({result['confidence']:.2f}) for image ID: {image_id}")
    except Exception as e:
        logger.error(f"DB write failed: {e}")

async def main():
    logger.info("Starting inference service")

    try:
        logger.info(f"Working directory: {os.getcwd()}")
        logger.info(f"Directory files: {', '.join(os.listdir('.'))}")

        config = load_config()
        logger.info("Config loaded successfully")

        kafka_config = {
            'bootstrap_servers': ','.join(config['kafka']['brokers']),
            'group_id': config['kafka'].get('group_id', 'videos'),
            'topic': config['kafka'].get('topic', 'videos')
        }

        pg_config = config['postgresql']
        pg_dsn = f"postgres://{pg_config['DATABASE_USER']}:{pg_config['DATABASE_PASSWORD']}@{pg_config['DATABASE_HOST']}:{pg_config['DATABASE_PORT']}/{pg_config['DATABASE_NAME']}"
        logger.info(f"Using PostgreSQL: {pg_config['DATABASE_HOST']}:{pg_config['DATABASE_PORT']}/{pg_config['DATABASE_NAME']}")

        ctx = mp.get_context("spawn")
        input_queue = ctx.Queue(maxsize=30)
        output_queue = ctx.Queue(maxsize=50)

        p = ctx.Process(target=inference_worker, args=(input_queue, output_queue), daemon=True)
        p.start()
        logger.info("Inference process started")

        executor = ThreadPoolExecutor(max_workers=4)

        tasks = [
            asyncio.create_task(consume_kafka(input_queue, kafka_config, executor)),
            asyncio.create_task(db_writer(output_queue, pg_dsn, executor))
        ]
        try:
            await asyncio.gather(*tasks)
        finally:
            input_queue.put(None)
            output_queue.put(None)
            for t in tasks:
                t.cancel()
            p.join(timeout=2)
            if p.is_alive(): p.terminate()
            logger.info("Shutdown complete")

    except KeyboardInterrupt:
        logger.info("Received KeyboardInterrupt, shutting down")

    except Exception as e:
        logger.exception(f"Service failed: {str(e)}")


if __name__ == "__main__":
    asyncio.run(main())