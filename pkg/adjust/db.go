package adjust

type Client struct {
	config Config
}

type AdjustConfig interface {
	GetIngestrURI() string
}

func NewClient(c Config) (*Client, error) {
	return &Client{
		config: c,
	}, nil
}

func (c *Client) GetIngestrURI() (string, error) {
	return c.config.GetIngestrURI(), nil
}
