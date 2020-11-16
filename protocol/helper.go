package protocol

import "errors"

var errDeserialize error = errors.New("Deserialization error")

func serializeString(str string, buf []byte) []byte {
	i := len(str)
	e := len(buf)
	buf = buf[:e+i+1]
	copy(buf[e:], str)
	buf[e+i] = 0
	return buf
}

func deserializeString(buf []byte, err *error) (string, []byte) {
	if *err != nil {
		return "", nil
	}
	for i := 0; i < len(buf); i++ {
		if buf[i] == 0 {
			return string(buf[:i]), buf[:i+1]
		}
	}
	*err = errDeserialize
	return "", nil
}
