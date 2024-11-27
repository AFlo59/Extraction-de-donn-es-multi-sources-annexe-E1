#!/usr/bin/env bats

setup() {
  # Créer un environnement de test temporaire
  TEST_DIR=$(mktemp -d)
  cp scripts/schedule.sh "$TEST_DIR/"
  cp script.py "$TEST_DIR/"
  cd "$TEST_DIR"
  
  # Créer des fichiers de test
  mkdir -p logs
  touch logs/schedule.log
}

teardown() {
  # Nettoyer après les tests
  rm -rf "$TEST_DIR"
}

@test "schedule.sh exécute le script principal avec succès" {
  run bash schedule.sh
  [ "$status" -eq 0 ]
  [ -f "logs/pipeline.log" ]
}

@test "schedule.sh gère les erreurs du script principal" {
  # Simuler une erreur dans script.py
  echo "import sys; sys.exit(1)" > script.py
  
  run bash schedule.sh
  [ "$status" -eq 0 ] # Le script devrait gérer l'erreur et continuer
  grep -q "Erreur lors de l'exécution de script.py." logs/schedule.log
}
