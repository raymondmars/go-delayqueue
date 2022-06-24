package message

type Command uint

const (
	Test Command = iota
	Push
	Subscribe
)
