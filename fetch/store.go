package fetch

// Placeholder until the spike proves Fuji serves ancient history.
type Store struct{}

func OpenStore(dir string) (*Store, error) { return &Store{}, nil }
func (s *Store) Close() error              { return nil }
