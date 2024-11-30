
from OpenSSL import crypto
from OpenSSL.crypto import _lib, _ffi, X509


class Certificate:

    def __init__(self, buff):
        self.content = []
        self._parse(buff)

    def get(self):
        return self.content

    def _parse(self, buff):
        pkcs7 = crypto.load_pkcs7_data(crypto.FILETYPE_ASN1, buff)

        certs_stack = _ffi.NULL
        if pkcs7.type_is_signed():
            certs_stack = pkcs7._pkcs7.d.sign.cert
        elif pkcs7.type_is_signedAndEnveloped():
            certs_stack = pkcs7._pkcs7.d.signed_and_enveloped.cert

        pycerts = []

        for i in range(_lib.sk_X509_num(certs_stack)):
            tmp = _lib.X509_dup(_lib.sk_X509_value(certs_stack, i))
            pycert = X509._from_raw_x509_ptr(tmp)
            pycerts.append(pycert)

        if not pycerts:
            return None

        for cert in pycerts:
            name = str(cert.get_subject())[19:-2].replace('/', ', ')
            md5 = cert.digest('md5').decode().replace(':', '')

            self.content.append((name, md5))
