package message

type ClientMessage struct {
	AuthToken string  `json:"auth_token"`
	CMD       Command `json:"cmd"`
	Contents  string  `json:"contents"`
}
