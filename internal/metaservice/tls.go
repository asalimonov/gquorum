package metaservice

import (
	"bytes"
	"crypto/tls"
	"slices"
)

// Config.Clone preserves TLS's private synchronization and ticket state, but
// shares exported slices. Own those values as well. Private keys, certificate
// leaves and custom callbacks/caches remain immutable borrowed dependencies.
func cloneTLS(input *tls.Config) *tls.Config {
	if input == nil {
		return nil
	}
	out := input.Clone()
	out.NextProtos = slices.Clone(input.NextProtos)
	out.CipherSuites = slices.Clone(input.CipherSuites)
	out.CurvePreferences = slices.Clone(input.CurvePreferences)
	out.EncryptedClientHelloConfigList = bytes.Clone(input.EncryptedClientHelloConfigList)
	out.EncryptedClientHelloKeys = slices.Clone(input.EncryptedClientHelloKeys)
	for i := range out.EncryptedClientHelloKeys {
		out.EncryptedClientHelloKeys[i].Config = bytes.Clone(input.EncryptedClientHelloKeys[i].Config)
		out.EncryptedClientHelloKeys[i].PrivateKey = bytes.Clone(input.EncryptedClientHelloKeys[i].PrivateKey)
	}
	if input.RootCAs != nil {
		out.RootCAs = input.RootCAs.Clone()
	}
	if input.ClientCAs != nil {
		out.ClientCAs = input.ClientCAs.Clone()
	}
	out.Certificates = slices.Clone(input.Certificates)
	for i := range out.Certificates {
		out.Certificates[i] = cloneCertificate(input.Certificates[i])
	}
	if input.NameToCertificate != nil {
		out.NameToCertificate = make(map[string]*tls.Certificate, len(input.NameToCertificate))
		for name, cert := range input.NameToCertificate {
			if cert == nil {
				out.NameToCertificate[name] = nil
				continue
			}
			owned := cloneCertificate(*cert)
			out.NameToCertificate[name] = &owned
		}
	}
	return out
}

func cloneCertificate(input tls.Certificate) tls.Certificate {
	out := input
	out.Certificate = cloneBytes(input.Certificate)
	out.SupportedSignatureAlgorithms = slices.Clone(input.SupportedSignatureAlgorithms)
	out.OCSPStaple = bytes.Clone(input.OCSPStaple)
	out.SignedCertificateTimestamps = cloneBytes(input.SignedCertificateTimestamps)
	return out
}

func cloneBytes(input [][]byte) [][]byte {
	out := slices.Clone(input)
	for i := range out {
		out[i] = bytes.Clone(input[i])
	}
	return out
}
