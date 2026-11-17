from unittest.mock import Mock

import pytest

from ingestion.sources.openaq import (
    OpenAQClient,
    OpenAQTransientError,
)


def test_get_rejects_non_object_json_payload():
    client = OpenAQClient.__new__(OpenAQClient)
    client.base_url = "https://api.openaq.org/v3"
    client.timeout = 10
    response = Mock(status_code=200)
    response.json.return_value = []
    client.session = Mock()
    client.session.get.return_value = response

    with pytest.raises(OpenAQTransientError, match="invalid JSON payload"):
        OpenAQClient._get.__wrapped__(client, "/locations")

    client.session.get.assert_called_once()
