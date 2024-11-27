#!/usr/bin/env bats

setup() {
  # Créer un environnement de test temporaire
  TEST_DIR=$(mktemp -d)
  cp scripts/install.sh "$TEST_DIR/"
  cd "$TEST_DIR"
}

teardown() {
  # Nettoyer après les tests
  rm -rf "$TEST_DIR"
}

@test "install.sh crée l'environnement virtuel" {
  run bash install.sh
  [ "$status" -eq 0 ]
  [ -d "venv" ]
}

@test "install.sh installe les dépendances Python" {
  run bash install.sh
  [ "$status" -eq 0 ]
  source venv/bin/activate
  pip show Pillow >/dev/null
  [ "$status" -eq 0 ]
}
