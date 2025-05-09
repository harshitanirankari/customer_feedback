import pytest
from unittest.mock import patch, MagicMock
from backend.chatbot import CustomerInsightsChatbot
import pandas as pd

@pytest.fixture
def chatbot():
    # Use auto_init=False to avoid actual Snowflake/LLM calls
    return CustomerInsightsChatbot(auto_init=False)

def test_initialization_creates_vector_store(chatbot):
    assert chatbot.vector_store is not None
    assert isinstance(chatbot.documents_metadata, dict)

from langchain_core.documents import Document

def test_chunk_documents(chatbot):
    docs = [Document(page_content='This is a test document.', metadata={})]
    chunks = chatbot.chunk_documents(docs)
    assert isinstance(chunks, list)
    assert all(hasattr(chunk, 'page_content') for chunk in chunks)

@patch('backend.chatbot.CustomerInsightsChatbot.connect_to_snowflake')
def test_fetch_snowflake_data(mock_connect, chatbot):
    # Mock the Snowflake connection and cursor
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = [(1, 'test', 'good', '2024-01-01')]
    mock_cursor.description = [('id',), ('text',), ('sentiment',), ('date',)]
    mock_conn.cursor.return_value = mock_cursor
    mock_connect.return_value = mock_conn

    with patch('backend.chatbot.snowflake.connector.connect', return_value=mock_conn):
        df = chatbot.fetch_snowflake_data()
        assert isinstance(df, pd.DataFrame)
        assert not df.empty

def test_process_snowflake_data(chatbot):
    df = pd.DataFrame({
        'AUTHOR': ['user1'],
        'SUBREDDIT': ['testsub'],
        'TEXT': ['This is a test'],
        'FEEDBACK_ID': ['id1'],
        'CREATED_DATE': ['2024-01-01'],
        'PERMALINK': ['http://example.com']
    })
    docs = chatbot.process_snowflake_data(df)
    assert isinstance(docs, list)
    assert hasattr(docs[0], 'page_content')
    assert 'This is a test' in docs[0].page_content

@patch('backend.chatbot.CustomerInsightsChatbot.add_documents_to_vector_store')
def test_import_csv_files(mock_add, chatbot, tmp_path):
    # Create a dummy CSV file
    csv_file = tmp_path / 'test.csv'
    csv_file.write_text('id,text,sentiment,date\n1,Test,good,2024-01-01')
    chatbot.import_csv_files([str(csv_file)])
    mock_add.assert_called()

def test_get_answer(chatbot):
    # Mock chain with a return value similar to what the real chain returns
    class DummyResult:
        content = '42'
    chatbot.chain = MagicMock()
    chatbot.chain.invoke.return_value = DummyResult()
    chatbot.has_data = MagicMock(return_value=True)  # Ensure it doesn't try to auto-init
    result = chatbot.get_answer('What is the answer?')
    assert 'answer' in result
    assert result['answer'] == '42'

def test_get_data_summary(chatbot):
    summary = chatbot.get_data_summary()
    assert isinstance(summary, dict)
    assert 'total_documents' in summary
