import yaml
from banal import ensure_dict, ensure_list


class Config:
    """
    metadata:
      dedup:
        max: published_at
        unique: foreign_id
      file_name: _file_name
      include:
      - reference
      - modified_at
      - title
      - originators
      - publisher:name
      unique: content_hash
    public:
      url: https://my_bucket.s3.eu-central-1.amazonaws.com/foo/bar/{_file_name}
      uri: s3://my_bucket/foo/bar/{_file_name}
    """

    def __init__(self, m):
        self._m = m
        self._config = {}
        config_path = m._backend.get_path("config.yml")
        if m._backend.exists(config_path):
            with open(config_path) as f:
                config = yaml.load(f)
            self._config = ensure_dict(config)
        self._metadata = ensure_dict(self["metadata"])
        self._public = ensure_dict(self["public"])

    def __getitem__(self, item):
        return self._config.get(item)

    def __contains__(self, item):
        return item in self._config

    @property
    def unique(self):
        return self._metadata.get("unique", "content_hash")

    @property
    def file_name(self):
        return self._metadata.get("file_name", self.unique)

    @property
    def required_keys(self):
        def _get_required_keys():
            yield self.unique
            yield self.file_name
            meta = ensure_dict(self["metadata"])
            if "dedup" in meta:
                for key in ensure_dict(meta["dedup"]).values():
                    yield key

        return set(_get_required_keys())

    @property
    def keys(self):
        def _get_keys():
            yield from self.required_keys
            for key in ensure_list(self._metadata.get("include")):
                yield key

        return set(_get_keys())

    def get_public(self, data):
        """
        compute a public url or uris with simple string replacement from
        `public` config
        """
        for key, value in self._public.items():
            yield key, value.format(**data)
