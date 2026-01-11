package kafka

type Config struct {
	BrokerAddress     string
	ControllerAddress string
	Topic             string
	Partitions        int
	GroupID           string
}
