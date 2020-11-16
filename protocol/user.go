package protocol

import (
	"errors"
	"strings"
)

// UserIdent identifes a user.
//
// <ns>/<host+castle>/<loard+minion>
//
// where <ns> is currently "dns" only.
// The part behind the plus-sign is optional.
// The string is encoded as UTF-8.
type UserIdent struct {
	// Can be NULL.
	// At most 32byte long excluding the trailing zero.
	Namespace string
	// At most 255byte long including the trailing zero,
	// because this is the DNS limit.
	Host string
	// Can be NULL.
	// At most 64byte long excluding the trailing zero.
	Castle string
	// At most 64byte long excluding the trailing zero.
	Lord string
	// Can be NULL.
	// At most 64byte long excluding the trailing zero.
	Minion string
}

// Serialize writes the UserIdent to the buffer and returns
// the amount the extended buffer.
// The buffer must have sufficient capacity.
func (u *UserIdent) Serialize(buffer []byte) []byte {
	buffer = serializeString(u.Namespace, buffer)
	buffer = serializeString(u.Host, buffer)
	buffer = serializeString(u.Castle, buffer)
	buffer = serializeString(u.Lord, buffer)
	buffer = serializeString(u.Minion, buffer)
	return buffer
}

// Deserialize reads a UserIdent from the buffer and returns
// the remaining buffer.
func (u *UserIdent) Deserialize(buffer []byte, err *error) []byte {
	if *err != nil {
		return nil
	}
	u.Namespace, buffer = deserializeString(buffer, err)
	u.Host, buffer = deserializeString(buffer, err)
	u.Castle, buffer = deserializeString(buffer, err)
	u.Lord, buffer = deserializeString(buffer, err)
	u.Minion, buffer = deserializeString(buffer, err)
	return buffer
}

// ByteCount returns the number of bytes required to serialize the object.
func (u *UserIdent) ByteCount() int {
	return len(u.Namespace) + 1 + len(u.Host) + 1 + len(u.Castle) + 1 + len(u.Lord) + 1 + len(u.Minion) + 1
}

func (u *UserIdent) String() string {
	var str string
	if len(u.Namespace) == 0 {
		str += "dns/"
	} else {
		str += u.Namespace + "/"
	}
	str += u.Host
	if len(u.Castle) != 0 {
		str += "+" + u.Castle + "/"
	} else {
		str += "/"
	}
	str += u.Lord
	if len(u.Minion) != 0 {
		str += "+" + u.Minion
	}
	return str
}

var errParsing error = errors.New("Identifier parsing error")

// ParseUserIdent parses the string representation of UserIdent
func ParseUserIdent(str string, u *UserIdent) (string, error) {
	i := strings.Index(str, "/")
	if i == -1 || i == 0 {
		return "", errParsing
	}
	u.Namespace = str[:i]
	str = str[i+1:]
	i = strings.Index(str, "/+")
	if i == -1 || i == 0 {
		return "", errParsing
	}
	u.Host = str[:i]
	if str[i] == '+' {
		str = str[i+1:]
		i = strings.Index(str, "/")
		if i == -1 || i == 9 {
			return "", errParsing
		}
		u.Castle = str[:i]
		str = str[:i+1]
	} else {
		str = str[:i+1]
	}
	i = strings.Index(str, "/+")
	if i == 0 {
		return "", errParsing
	}
	if i == -1 {
		i = len(str)
		u.Lord = str
	} else if str[i] == '+' {
		u.Lord = str[:i]
		str = str[i+1:]
		i = strings.Index(str, "/")
		if i == 0 {
			return "", errParsing
		}
		if i == -1 {
			i = len(str)
		}
		u.Minion = str[:i]
	}
	return str[i:], nil
}
