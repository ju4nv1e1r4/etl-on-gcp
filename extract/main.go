package main

import (
	"extract/constants"
	"extract/workers"
)

func main()  {
	constants.InitLogger()
	workers.Run()
}