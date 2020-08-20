import main
import pytest
import pandas as pd
from sqlalchemy import create_engine


class TestTrueFilmETL:
    @pytest.mark.parametrize(
        "unclean_str, clean_str",
        [
            ("https://en.wikipedia.org/wiki/Abraham_Lincoln", "wiki/Abraham_Lincoln"),
            ("https://en.wikipedia.org/wiki/Home_Alone", "wiki/Home_Alone"),
            ("Wikipedia: Isn't She Great", "Isn't She Great"),
            (
                "Wikipedia: Harry Potter and the Philosopher's Stone",
                "Harry Potter and the Philosopher's Stone",
            ),
        ],
    )
    def test_clean_str(self, unclean_str, clean_str):
        assert main.clean_str(unclean_str) == clean_str

    @pytest.mark.parametrize(
        "df, expected_count",
        [
            (main.load_imdb_movies_data(), 45383),
            (main.load_wiki_movies_data(), 123703695),
            (main.load_data(), 1000),
        ],
    )
    def test_dataframe_record_counts(self, df, expected_count):
        assert len(df) == expected_count

    @pytest.mark.parametrize(
        "df, expected_cols",
        [
            (
                main.load_imdb_movies_data(),
                [
                    "budget",
                    "id",
                    "imdb_id",
                    "production_companies",
                    "release_date",
                    "revenue",
                    "title",
                    "vote_average",
                    "vote_count",
                    "ratio",
                ],
            ),
            (main.load_wiki_movies_data(), ["title", "url", "abstract"]),
            (
                main.load_data(),
                [
                    "title",
                    "budget",
                    "release_date",
                    "revenue",
                    "rating",
                    "ratio",
                    "production_company",
                    "wikipedia_url",
                    "wikipedia_abstract",
                    "updated_on_utc",
                ],
            ),
        ],
    )
    def test_dataframe_columns(self, df, expected_cols):
        assert list(df.columns) == expected_cols

    def test_postgresql_table_not_empty(self):
        main.load_postgres(data=main.load_data(), table_name="movies")
        sql = "select count(1) from movies;"
        expected_row_count = 1000
        df = pd.read_sql_query(sql, con=create_engine(main.POSTGRESQL_CONN))
        assert df['count'][0] == expected_row_count
