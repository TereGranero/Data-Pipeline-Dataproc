import flask
import pytest

import main

@pytest.fixture(scope="module")
def app():
  return flask.Flask(__name__)

@pytest.mark.freeze_time('2021-11-25T11:14:58.729821')
def test_response_contains_identifier_in_json(app, mocker):
  mocker.patch('main.upload')
  with app.test_request_context(json={'pokemonId': 'pikachu'}):
    res = main.hello_storage(flask.request)
    assert res.content_type == 'application/json'
    assert '{"ok":42}' in str(res.data)

@pytest.mark.freeze_time('2021-11-25T11:14:58.729821')
def test_creates_file_using_pokemon_id(app, mocker):
  mocker.patch('main.upload')
  with app.test_request_context(json={'pokemonId': 'pikachu'}):
    res = main.hello_storage(flask.request)
    main.upload.assert_called_once_with('pokemons/pokemon-pikachu.json', '{"pokemonId": "pikachu", "created": "2021-11-25T11:14:58.729821"}')
