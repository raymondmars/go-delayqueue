package message

type Command uint

const (
	Test Command = iota + 1
	Push
	Subscribe
)
