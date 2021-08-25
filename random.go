package main

import (
	"crypto/rand"
	"encoding/hex"
)

const tokenLength int = 20

func UniqueToken() (string, error) {
	b := make([]byte, tokenLength)

	_, err := rand.Read(b)
	if err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}