package goutils

import (
	"crypto/md5"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"io"
	"net"
	"os"
	"strings"
)

func GetMd5String(s string) string {
	h := md5.New()
	h.Write([]byte(s))
	return hex.EncodeToString(h.Sum(nil))
}

func GetGuid() string {
	b := make([]byte, 48)
	if _, err := io.ReadFull(rand.Reader, b); err != nil {
		return ""
	}
	return GetMd5String(base64.URLEncoding.EncodeToString(b))
}

func GetLocalIP() string {
	host, err := os.Hostname()
	if err == nil && host != "localhost" {
		ips, _ := net.LookupIP(host)
		for _, a := range ips {
			if a.IsLoopback() {
				continue
			}
			ip := a.String()
			if isLocalIPWithoutLoop(ip) {
				return ip
			}
		}
	}

	addrs, _ := net.InterfaceAddrs()
	for _, a := range addrs {
		if ipnet, ok := a.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil && isLocalIPWithoutLoop(ipnet.IP.String()) {
				return ipnet.IP.String()
			}
		}
	}

	return ""
}

func isLocalIPWithoutLoop(ip string) bool {
	if strings.HasPrefix(ip, "10.") || strings.HasPrefix(ip, "192.168.") || strings.HasPrefix(ip, "172.") {
		return true
	}
	return false
}
