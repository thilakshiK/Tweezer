import pytest

from test_app import create_app

@pytest.fixture
def app():
    app = create_app()
    app.debug = True
    return app.test_client()


@pytest.mark.home
def test_homepage(app):
    response = app.get("/")
    assert response.status_code == 200
    assert b"Tweezer" in response.data


@pytest.mark.worldmap
def test_map(app):
    response = app.get("/map")
    assert response.status_code == 200
    assert b"Map" in response.data


@pytest.mark.timeline
def test_timeline(app):
    response = app.get("/timeline")
    assert response.status_code == 200
    assert b"Timeline" in response.data


@pytest.mark.tweets
def test_tweets(app):
    response = app.get("/tweets")
    assert response.status_code == 200
    assert b"TWEETS" in response.data


@pytest.mark.piechart
def test_piechart(app):
    response = app.get("/piechart")
    assert response.status_code == 200
    assert b"Pie Chart" in response.data
