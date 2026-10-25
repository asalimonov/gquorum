package model

// UnmarshalJSON applies recovery's bounded exact-key/duplicate-field rules even
// when the optional record is decoded inside an ordinary schema-4 FSM snapshot.
func (s *ImportState) UnmarshalJSON(data []byte) error {
	type wire ImportState
	var decoded wire
	if err := DecodeStrict(data, &decoded, ManifestMaxBytes); err != nil {
		return err
	}
	*s = ImportState(decoded)
	return nil
}
