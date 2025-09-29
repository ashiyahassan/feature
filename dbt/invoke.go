package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"os/exec"
	"strings"
)

func handler(w http.ResponseWriter, r *http.Request) {
	log.Print("Simple DBT Pipeline: received a request")

	// Expects: model_name,env_name
	// The Cloud Workflow will pass this as arg1=model_name,env_name
	argStr := r.URL.Query().Get("arg1")

	if argStr == "" {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, "Please provide the comma-separated args: model_name,env_name")
		return
	}

	parts := strings.Split(argStr, ",")
	// Ensure we have at least model_name and env_name
	if len(parts) < 2 {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, "Missing arguments. Format is: model_name,env_name")
		return
	}

	modelName := parts[0]
	envName := parts[1]

	log.Printf("DBT Model: %s, Environment: %s", modelName, envName)

	// Execute the shell script with the model name ($1) and environment ($2)
	cmd := exec.CommandContext(r.Context(), "/bin/sh", "script.sh", modelName, envName)

	// Direct script errors to standard error

	// Capture the combined output (stdout and stderr) for debugging
	out, err := cmd.CombinedOutput()

	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		errorOutput := fmt.Sprintf("Error executing dbt: %s\nOutput: %s", err, string(out))
		log.Print(errorOutput)
		fmt.Fprintf(w, errorOutput)
		return
	}

	log.Print("DBT Pipeline Executed successfully!")
	w.Write(out)
}

func main() {
	log.Print("Simple DBT Pipeline: starting server...")

	http.HandleFunc("/", handler)

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	log.Printf("Listening on %s", port)
	// Cloud Run requires the application to listen on the $PORT environment variable
	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%s", port), nil))
}
