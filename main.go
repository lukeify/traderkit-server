package main

import (
	"log"
	"os"

	"traderkit-server/utils"

	"github.com/gofiber/fiber/v2"
)

func main() {
	if err := utils.LoadEnvFile(); err != nil {
		os.Exit(1)
	}
	app := fiber.New()

	app.Get("/", func(c *fiber.Ctx) error {
		return c.SendString("Hello, World!")
	})

	log.Fatal(app.Listen(":3000"))
}
