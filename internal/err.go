package mqttdistribnet_int

type DistributionNetworkError struct{ Message string }

func (m *DistributionNetworkError) Error() string { return m.Message }
