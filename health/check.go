package health

import "context"

const (
	StatusUp   = "UP"
	StatusDown = "DOWN"
)

func Check(ctx context.Context, services []Checker) Health {
	health := Health{}
	health.Status = StatusUp
	healths := make(map[string]Health)
	for _, service := range services {
		sub := Health{}
		c := context.Background()
		d0, err := service.Check(c)
		if err == nil {
			sub.Status = StatusUp
			if d0 != nil && len(d0) > 0 {
				sub.Data = d0
			}
		} else {
			sub.Status = StatusDown
			health.Status = StatusDown
			if d0 != nil {
				data := service.Build(c, d0, err)
				if data != nil && len(data) > 0 {
					sub.Data = data
				}
			} else {
				data := make(map[string]interface{}, 0)
				data["error"] = err.Error()
				sub.Data = data
			}
		}
		healths[service.Name()] = sub
	}
	if len(healths) > 0 {
		health.Details = healths
	}
	return health
}
