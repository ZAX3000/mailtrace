# tests/blueprints/test_assets_bp.py
def test_missing_css_returns_empty(client):
    r = client.get("/assets/does-not-exist.css")
    assert r.status_code == 200 and r.mimetype == "text/css" and r.data == b""

def test_missing_js_returns_placeholder(client):
    r = client.get("/assets/does-not-exist.js")
    assert r.status_code == 200 and r.mimetype == "application/javascript"