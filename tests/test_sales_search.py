"""Local-only Sales Search provider and router tests.

No Google request or production database connection is allowed in this module;
the router receives a deterministic provider and the Google adapter receives a
fake response.
"""

from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient

from app.api.sales import build_sales_router
from app.sales_search import (
    GooglePlacesProvider,
    MockSalesSearchProvider,
    SalesSearchQuery,
    SalesSearchProviderError,
    build_sales_search_provider,
)


RESULTS = [{
    "place_id": "ChIJlocal1",
    "name": "Local Coffee",
    "address": "Київ, Хрещатик 1",
    "rating": 4.8,
    "reviews_count": 312,
    "website": "https://local.example",
    "phone": "+380441112233",
    "google_maps_url": "https://maps.google.com/?cid=1",
    "source": "mock",
}]


def _client(provider):
    def verify_actor(authorization, init_data):
        if authorization == "Bearer sales-superadmin":
            return {"user_id": 1, "telegram_id": 9001}
        raise HTTPException(401, detail={"code": "UNAUTHORIZED"})

    app = FastAPI()
    app.include_router(build_sales_router(
        verify_actor=verify_actor,
        superadmin_telegram_ids={9001},
        search_provider=provider,
    ))
    return TestClient(app)


def test_mock_provider_search_is_authenticated_and_normalized():
    provider = MockSalesSearchProvider(RESULTS)
    with _client(provider) as client:
        response = client.get(
            "/sales/search?city=%20%D0%9A%D0%B8%D1%97%D0%B2%20&radius_km=12&category=cafe&name=Local&limit=1",
            headers={"Authorization": "Bearer sales-superadmin"},
        )
    assert response.status_code == 200, response.text
    body = response.json()
    assert body["provider"] == "mock"
    assert body["items"] == RESULTS
    assert provider.queries == [SalesSearchQuery("Київ", 12, "cafe", "Local", 1)]


def test_search_requires_superadmin_and_rejects_unknown_query_fields():
    provider = MockSalesSearchProvider(RESULTS)
    with _client(provider) as client:
        assert client.get("/sales/search?city=Kyiv").status_code == 401
        response = client.get(
            "/sales/search?city=Kyiv&shop_id=1",
            headers={"Authorization": "Bearer sales-superadmin"},
        )
    assert response.status_code == 422
    assert response.json()["detail"]["code"] == "UNEXPECTED_PARAMETER"


def test_google_provider_maps_places_without_network_or_exposing_key():
    calls = []

    class FakeResponse:
        status_code = 200

        @staticmethod
        def json():
            return {"places": [{
                "id": "ChIJgoogle",
                "displayName": {"text": "Google Coffee"},
                "formattedAddress": "Kyiv, 1 Main St",
                "rating": 4.7,
                "userRatingCount": 88,
                "websiteUri": "https://google.example",
                "nationalPhoneNumber": "+380441234567",
                "googleMapsUri": "https://maps.google.com/?cid=2",
            }]}

    def fake_request(url, headers, payload):
        calls.append((url, headers, payload))
        return FakeResponse()

    provider = GooglePlacesProvider("local-test-key", request=fake_request)
    results = provider.search(SalesSearchQuery("Київ", 10, "cafe", None, 20))
    assert results[0]["place_id"] == "ChIJgoogle"
    assert results[0]["source"] == "google_places"
    assert calls[0][0].endswith("places:searchText")
    assert calls[0][1]["X-Goog-Api-Key"] == "local-test-key"
    assert calls[0][2] == {
        "textQuery": "cafe, Київ", "includedType": "cafe", "pageSize": 20,
        "languageCode": "uk",
    }


def test_google_provider_without_key_fails_closed():
    provider = GooglePlacesProvider("")
    try:
        provider.search(SalesSearchQuery("Київ", 10, "cafe", None, 20))
    except SalesSearchProviderError as error:
        assert error.code == "SALES_SEARCH_PROVIDER_NOT_CONFIGURED"
        assert error.status_code == 503
    else:
        raise AssertionError("provider without GOOGLE_PLACES_API_KEY must not search")


def test_default_provider_is_google_adapter_and_never_a_mock():
    provider = build_sales_search_provider()
    assert provider.name == "google_places"
