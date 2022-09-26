package mysql

const (
	CFG_HOST = "HOST"
	CFG_PORT = "PORT"
)

type Config struct {
	Host     string `json:"host"`
	Port     int    `json:"port"`
	User     string `json:"username"`
	Pass     string `json:"password"`
	ServerId int    `json:"serverId"`

	LogFile  string `json:"binlogFilename"`
	Position int    `json:"binlogPosition"`

	Protocol string `json:"protocol"`
}
