package postgresql

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	client2 "orchestrator/internal/postgresql/client"
)

type PgClient struct {
	client client2.Client
	logger *log.Logger
}

func (r *PgClient) GetTeamMembers(ctx context.Context, heroIDs []string) ([]domain.TeamMember, error) {
	query := "SELECT title, category, lvl, mana FROM heroes WHERE id = ANY($1)"
	rows, err := r.client.Query(ctx, query, heroIDs)
	if err != nil {
		return nil, fmt.Errorf("ошибка запроса героев: %w", err)
	}
	defer rows.Close()

	var team []domain.TeamMember
	for rows.Next() {
		var member domain.TeamMember
		var mana float64
		if err := rows.Scan(&member.Name, &member.Category, &member.Level, &mana); err != nil {
			return nil, fmt.Errorf("ошибка чтения данных героя: %w", err)
		}
		member.ManaCapacity = int(mana)
		team = append(team, member)
	}
	return team, nil
}

// GetChronicExpeditions выбирает документы экспедиций с пагинацией и формирует структуру Expedition.
func (r *PgClient) GetChronicExpeditions(ctx context.Context, limit, offset int) ([]domain.Expedition, error) {
	query := `
		SELECT id, tasks_id, hero_map, calculated_mana
		FROM expeditions
		ORDER BY id
		LIMIT $1 OFFSET $2
	`
	rows, err := r.client.Query(ctx, query, limit, offset)
	if err != nil {
		return nil, fmt.Errorf("ошибка запроса экспедиций: %w", err)
	}
	defer rows.Close()

	var expeditions []domain.Expedition
	for rows.Next() {
		var tasksID []string
		var heroMapJSON []byte
		var calculatedMana float64
		var id string // Можно использовать для отладки или логирования

		if err := rows.Scan(&id, &tasksID, &heroMapJSON, &calculatedMana); err != nil {
			return nil, fmt.Errorf("ошибка чтения данных экспедиции: %w", err)
		}

		var exp domain.Expedition
		// Количество задач — длина массива tasks_id.
		exp.CountTasks = len(tasksID)

		// Получаем количество подзадач суммированием длин массива ids в tasks.
		subtasksQuery := `SELECT COALESCE(SUM(array_length(grimoire_ids, 1)), 0) FROM tasks WHERE id = ANY($1)`
		var countSubtasks int
		if err := r.client.QueryRow(ctx, subtasksQuery, tasksID).Scan(&countSubtasks); err != nil {
			return nil, fmt.Errorf("ошибка подсчёта подзадач для экспедиции id=%d: %w", id, err)
		}
		exp.CountSubtasks = countSubtasks

		exp.ManaRequired = int(calculatedMana)

		// Из hero_map получаем список hero id.
		var heroMap map[string]int
		if err := json.Unmarshal(heroMapJSON, &heroMap); err != nil {
			return nil, fmt.Errorf("ошибка разбора hero_map для экспедиции id=%d: %w", id, err)
		}
		var heroIDs []string
		for heroID := range heroMap {
			heroIDs = append(heroIDs, heroID)
		}

		// Получаем данные о героях (команду) по heroIDs.
		team, err := r.GetTeamMembers(ctx, heroIDs)
		if err != nil {
			return nil, fmt.Errorf("ошибка получения команды для экспедиции id=%d: %w", id, err)
		}
		exp.Team = team

		expeditions = append(expeditions, exp)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return expeditions, nil
}

func (r *PgClient) InsertTasks(ctx context.Context, tasks []domain.Task) ([]string, error) {
	insertedIDs := make([]string, 0, len(tasks))
	query := `
		INSERT INTO tasks (difficulty, grimoire_ids)
		VALUES ($1, $2)
		RETURNING id
	`
	for _, task := range tasks {
		var insertedID string
		err := r.client.QueryRow(ctx, query, task.Difficulty, task.Ids).Scan(&insertedID)
		if err != nil {
			return nil, fmt.Errorf("ошибка при вставке задачи: %w", err)
		}
		insertedIDs = append(insertedIDs, insertedID)
	}
	return insertedIDs, nil
}

func (r *PgClient) InsertSubtask(ctx context.Context, task domain.Subtask) (string, error) {
	query := `
		INSERT INTO grimoire (title, difficulty, mana, strategy, magic, combat, visual)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		RETURNING id
	`

	err := r.client.QueryRow(ctx, query, task.Title, task.Difficulty, task.Mana, task.Strategy, task.Magic, task.Combat, task.Visual).Scan(&task.Id)
	if err != nil {
		return "", fmt.Errorf("ошибка при вставке задачи: %w", err)
	}
	return task.Id, nil
}

// Суммируем манну из гримуара для каждой задачи по её массиву id.
func (r *PgClient) SumManaForTasks(ctx context.Context, tasks []domain.Task) (float64, error) {
	var totalMana float64
	query := `
		SELECT COALESCE(SUM(mana), 0)
		FROM grimoire
		WHERE id = ANY($1) AND difficulty = $2
	`
	for _, task := range tasks {
		var manaSum float64
		err := r.client.QueryRow(ctx, query, task.Ids, task.Difficulty).Scan(&manaSum)
		if err != nil {
			return 0, fmt.Errorf("ошибка при суммировании манны для задачи с difficulty=%d: %w", task.Difficulty, err)
		}
		totalMana += manaSum
	}
	return totalMana, nil
}

// Вставляем документ в коллекцию expeditions.
func (r *PgClient) InsertExpedition(ctx context.Context, taskIDs []string, heroMap map[string]int, calculatedMana float64) error {
	// Преобразуем heroMap в JSON (для формата JSONB).
	heroMapJSON, err := json.Marshal(heroMap)
	if err != nil {
		return fmt.Errorf("ошибка при маршалинге heroMap: %w", err)
	}

	query := `
		INSERT INTO expeditions (tasks_id, hero_map, calculated_mana)
		VALUES ($1, $2, $3)
	`
	_, err = r.client.Exec(ctx, query, taskIDs, heroMapJSON, calculatedMana)
	if err != nil {
		return fmt.Errorf("ошибка при вставке экспедиции: %w", err)
	}
	return nil
}

func (r *PgClient) GetManaMatrix(ctx context.Context, req []domain.Task) ([][]float64, error) {
	// Определяем сложности в нужном порядке
	difficulties := []int{1, 2, 3}
	//categories := []string{"strategy", "magic", "combat", "visual"}

	// Инициализируем матрицу 3x4
	matrix := make([][]float64, 4)
	for i := range matrix {
		matrix[i] = make([]float64, 3)
	}

	// Обрабатываем входные данные
	for _, task := range req {
		// Определяем индекс сложности
		var difficultyIndex int
		found := false
		for i, diff := range difficulties {
			if task.Difficulty == diff {
				difficultyIndex = i
				found = true
				break
			}
		}
		if !found {
			return nil, fmt.Errorf("неизвестная сложность: %s", task.Difficulty)
		}

		// Формируем SQL-запрос
		query := fmt.Sprintf(`
			SELECT SUM(strategy), SUM(magic), SUM(combat), SUM(visual)
			FROM grimoire
			WHERE id = ANY($1) AND difficulty = $2
		`)

		var strategy, magic, combat, visual float64
		err := r.client.QueryRow(ctx, query, task.Ids, task.Difficulty).Scan(&strategy, &magic, &combat, &visual)
		if err != nil {
			return nil, fmt.Errorf("ошибка при выполнении запроса: %w", err)
		}

		// Заполняем матрицу
		matrix[0][difficultyIndex] += strategy
		matrix[1][difficultyIndex] += magic
		matrix[2][difficultyIndex] += combat
		matrix[3][difficultyIndex] += visual
	}

	return matrix, nil
}

func (r *PgClient) GetHeroes(ctx context.Context) ([]domain.Hero, error) {
	query := "SELECT id, title, category, lvl, mana FROM heroes"
	fmt.Println("GetHeroes started")
	rows, err := r.client.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("[GetHeroes] ошибка при выполнении запроса: %w", err)
	}
	defer rows.Close()

	var heroes []domain.Hero

	for rows.Next() {
		var hero domain.Hero
		// Указываем каждое поле структуры отдельно
		if err := rows.Scan(
			&hero.Id,
			&hero.Title,
			&hero.Category,
			&hero.Lvl,
			&hero.Mana,
		); err != nil {
			return nil, fmt.Errorf("[GetHeroes] ошибка при чтении данных из строки: %w", err)
		}
		heroes = append(heroes, hero)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("ошибка при итерации по строкам: %w", err)
	}
	fmt.Println("GetHeroes finished")
	return heroes, nil
}

func (r *PgClient) GetTasks(ctx context.Context) ([]string, error) {
	query := "SELECT job_type FROM tasks"
	rows, err := r.client.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("[GetTasks] ошибка при выполнении запроса: %w", err)
	}
	defer rows.Close()

	var tasks []string

	for rows.Next() {
		var task string
		if err := rows.Scan(&task); err != nil {
			return nil, fmt.Errorf("ошибка при чтении данных из строки: %w", err)
		}
		tasks = append(tasks, task)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("ошибка при итерации по строкам: %w", err)
	}

	return tasks, nil
}

func (r *PgClient) GetSubtasksByDifficulty(ctx context.Context, difficulty int) ([]domain.Subtask, error) {
	query := `
		SELECT g.id, g.title, g.difficulty, g.mana, g.strategy, g.magic, g.combat, g.visual
		FROM grimoire g
		WHERE g.difficulty = $1
	`

	rows, err := r.client.Query(ctx, query, difficulty)
	if err != nil {
		return nil, fmt.Errorf("ошибка при выполнении запроса: %w", err)
	}
	defer rows.Close()

	var entries []domain.Subtask

	for rows.Next() {
		var entry domain.Subtask
		if err := rows.Scan(&entry.Id, &entry.Title, &entry.Difficulty, &entry.Mana,
			&entry.Strategy, &entry.Magic, &entry.Combat, &entry.Visual); err != nil {
			return nil, fmt.Errorf("ошибка при сканировании данных: %w", err)
		}
		entries = append(entries, entry)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("ошибка при итерации по строкам: %w", err)
	}

	return entries, nil
}

func (r *PgClient) RegisterUser(ctx context.Context, user domain.User) (int, error) {
	query := `
		INSERT INTO users (login, hash_password, role)
		VALUES ($1, $2, $3)
		RETURNING id
	`
	var id int
	err := r.client.QueryRow(ctx, query, user.Login, user.HashPassword, user.Role).Scan(&id)
	if err != nil {
		return 0, fmt.Errorf("ошибка при регистрации пользователя: %w", err)
	}
	return id, nil
}

func (r *PgClient) GetUserByLogin(ctx context.Context, login string) (domain.User, error) {
	query := `
		SELECT id, login, hash_password, role
		FROM users
		WHERE login = $1
	`
	var user domain.User
	err := r.client.QueryRow(ctx, query, login).Scan(&user.ID, &user.Login, &user.HashPassword, &user.Role)
	if err != nil {
		return user, fmt.Errorf("пользователь не найден: %w", err)
	}
	return user, nil
}

func NewPgClient(client client2.Client, logger *log.Logger) *PgClient {
	return &PgClient{client, logger}
}
