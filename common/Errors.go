package common

import "github.com/pkg/errors"

var (
	ERR_LOCK_ALREADY_REQUIRE = errors.New("锁已被占用")

	ERR_NET_IP_IS_NOT_EXSIST = errors.New("无本地网卡")
)
