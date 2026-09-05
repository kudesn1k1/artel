package simtest

import (
	"fmt"
	"strconv"
	"strings"
)

func parseCounterOp(op string) (int, error) {
	op = strings.Trim(op, " \t")
	parts := strings.Split(op, ":")
	if len(parts) != 2 {
		return 0, fmt.Errorf("invalid counter op format: %s", op)
	}

	val, err := strconv.Atoi(parts[1])
	if err != nil {
		return 0, fmt.Errorf("invalid counter op value: %s", op)
	}

	switch parts[0] {
	case "inc":
		return val, nil
	case "dec":
		return -val, nil
	default:
		return 0, fmt.Errorf("invalid counter op value: %s", op)
	}
}
