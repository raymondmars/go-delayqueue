package notify

type NotifyMode uint

const (
	HTTP NotifyMode = iota + 1
	SubPub
)

func BuildExecutor(mode NotifyMode) Executor {
	switch mode {
	case HTTP:
		return NewHttpNotify()
	case SubPub:
		return &pubNotify{}
	default:
		return nil
	}
}
