package influxdb

func contains(list []string, item string) bool {
	return indexOf(list, item) != -1
}

func indexOf(list []string, item string) int {
	for i, x := range list {
		if x == item {
			return i
		}
	}

	return -1
}
