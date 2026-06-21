package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/go-redis/redis"
	"github.com/gorilla/mux"

	_ "github.com/lib/pq"
)

type Job struct {
	Name     string `json:"name"`
	Payload  string `json:"payload"`
	Priority int    `json:"priority,omitempty"`
}

const (
	DefaultPriority = 5
	MinPriority     = 1
	MaxPriority     = 10

	// PriorityScoreMultiplier separates priority bands in the Redis sorted
	// set score. priority * 1e13 + unix_millis stays well within float64
	// integer precision (2^53 ≈ 9e15) for ~317 years.
	PriorityScoreMultiplier = 1e13
)

var db *sql.DB
var redisClient *redis.Client

func main() {
	dbUrl := os.Getenv("DATABASE_URL")

	if dbUrl == "" {
		panic("DB URL not found")
	}

	var err error

	// Try 10 times, with 3s sleep to connect to database
	for i := 0; i < 10; i++ {
		db, err = sql.Open("postgres", dbUrl)

		if err == nil {
			err = db.Ping()
			if err == nil {
				// Connection successful
				break
			}
		}

		fmt.Println("Waiting for database to be ready...")
		time.Sleep(3 * time.Second)
	}

	if err != nil {
		panic(err)
	}

	defer db.Close()

	// Setup Redis Client
	redisAddr := os.Getenv("REDIS_ADDR")

	if redisAddr == "" {
		panic("Redis URL not found")
	}

	redisClient = redis.NewClient(&redis.Options{
		Addr: redisAddr,
	})

	if err := redisClient.Ping().Err(); err != nil {
		panic("Could not connect to Redis: " + err.Error())
	}

	// Set HTTP Server
	r := mux.NewRouter()
	r.HandleFunc("/submit_job", createJobHandler).Methods("POST")

	fmt.Println("Scheduler service is running on :8000")
	log.Fatal(http.ListenAndServe(":8000", r))
}

func createJobHandler(w http.ResponseWriter, r *http.Request) {
	var job Job

	if err := json.NewDecoder(r.Body).Decode(&job); err != nil {
		http.Error(w, "Invalid request payload", http.StatusBadRequest)
		return
	}

	priority := job.Priority
	if priority == 0 {
		priority = DefaultPriority
	}
	if priority < MinPriority || priority > MaxPriority {
		http.Error(w, fmt.Sprintf("priority must be between %d and %d", MinPriority, MaxPriority), http.StatusBadRequest)
		return
	}

	var jobID int

	err := db.QueryRow(
		"INSERT INTO jobs (name, payload, priority) VALUES ($1, $2, $3) RETURNING id",
		job.Name, job.Payload, priority,
	).Scan(&jobID)

	if err != nil {
		http.Error(w, "Failed to create job", http.StatusInternalServerError)
		fmt.Println("Error inserting job", err)
		return
	}

	w.WriteHeader(http.StatusCreated)
	w.Write([]byte("Job created successfully"))

	fmt.Println("Created job", job.Name, "id", jobID, "priority", priority)

	jobWithID := map[string]interface{}{
		"id":       fmt.Sprintf("%d", jobID),
		"name":     job.Name,
		"payload":  job.Payload,
		"priority": priority,
	}

	jobJson, _ := json.Marshal(jobWithID)

	// Score = priority band + submission timestamp.
	// Lower score pops first (BZPOPMIN), so smaller priority value = higher
	// dispatch priority, with FIFO ordering within each priority band.
	score := float64(priority)*PriorityScoreMultiplier + float64(time.Now().UnixMilli())

	err = redisClient.ZAdd("job_queue", redis.Z{
		Score:  score,
		Member: jobJson,
	}).Err()

	if err != nil {
		fmt.Println("Error pushing job to redis:", err)
	}
}
