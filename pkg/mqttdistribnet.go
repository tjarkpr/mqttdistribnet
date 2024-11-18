package mqttdistribnet

type IDistributionNetworkManager interface {
	Purge() error
	Delete() error
	Close() error
}
