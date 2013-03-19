
    def _auto_encode(self, content_encoding, value):
        """Based upon the value of the content_encoding, encode the value.

        :param str content_encoding: The content encoding type (gzip, bzip2)
        :param str value: The value to encode
        :rtype: value

        """
        if content_encoding == 'gzip':
            return self._encode_gzip(value)

        if content_encoding == 'bzip2':
            return self._encode_bz2(value)

        logger.warning('Invalid content-encoding specified for auto-encoding')
        return value

    def _encode_bz2(self, value):
        """Return a bzip2 compressed value

        :param str value: Uncompressed value
        :rtype: str

        """
        return bz2.compress(value)

    def _encode_gzip(self, value):
        """Return zlib compressed value

        :param str value: Uncompressed value
        :rtype: str

        """
        return zlib.decompress(value)


