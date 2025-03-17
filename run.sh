#!/bin/bash
set -e

GREEN="\033[1;32m"
YELLOW="\033[1;33m"
CYAN="\033[1;36m"
RESET="\033[0m"

echo -e "${GREEN}Pipeline execution started${RESET}"

echo -e "${YELLOW}Extracting data...${RESET}"
cd extract && go run main.go && cd ..

echo -e "${YELLOW}Transforming data...${RESET}"
python3 transform/src/fs.py

echo -e "${CYAN}Building feature store and loading data on Google Cloud Storage Bucket...${RESET}"
python3 load/src/load.py

echo -e "${GREEN}Pipeline executed successfully!!${YELLOW}"