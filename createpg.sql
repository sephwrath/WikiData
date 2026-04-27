DROP TABLE IF EXISTS parsed_event CASCADE;
DROP TABLE IF EXISTS article_section_ext_text CASCADE;
DROP TABLE IF EXISTS article_section_link CASCADE;
DROP TABLE IF EXISTS article_section_format CASCADE;
DROP TABLE IF EXISTS article_section CASCADE;
DROP TABLE IF EXISTS article CASCADE;
DROP TABLE IF EXISTS dump_file CASCADE;


-- =========================
-- dump_file
-- =========================
CREATE TABLE dump_file (
    id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    file_name VARCHAR(400) NOT NULL,
    tar_info BYTEA NOT NULL,
    file_offset BIGINT NOT NULL,
    offset_data BIGINT NOT NULL,
    UNIQUE (file_name)
);


-- =========================
-- article
-- =========================
CREATE TABLE article (
    id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    title VARCHAR(400) NOT NULL UNIQUE,
    title_srch VARCHAR(400),
    description VARCHAR(1000),
    file_update TIMESTAMP NOT NULL,
    dump_file_id INTEGER,
    dump_idx INTEGER,
    url VARCHAR(600) NOT NULL,
    redirect VARCHAR(600),
    no_dates BOOLEAN,
    wiki_update_ts TIMESTAMP,
    err VARCHAR(30),

    CONSTRAINT fk_article_dump_file
        FOREIGN KEY (dump_file_id)
        REFERENCES dump_file(id)
        ON DELETE SET NULL
);

CREATE INDEX idx_article_dump_file_id
    ON article(dump_file_id);

ALTER TABLE article
ADD COLUMN title_srch_tsv tsvector;

UPDATE article
SET title_srch_tsv = to_tsvector('english', COALESCE(title_srch, ''));

CREATE INDEX idx_article_fulltext
ON article USING GIN (title_srch_tsv);

CREATE FUNCTION article_tsv_trigger() RETURNS trigger AS $$
BEGIN
  NEW.title_srch_tsv :=
    to_tsvector('english', COALESCE(NEW.title_srch, ''));
  RETURN NEW;
END
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_article_tsv
BEFORE INSERT OR UPDATE ON article
FOR EACH ROW EXECUTE FUNCTION article_tsv_trigger();

CREATE TABLE article_section (
    article_id INTEGER NOT NULL,
    section_id INTEGER NOT NULL,
    ext_text_count INTEGER,
    parent_section_id INTEGER,
    row_idx INTEGER,
    column_idx INTEGER,
    row_span INTEGER,
    column_span INTEGER,
    tag VARCHAR(40) NOT NULL,
    format VARCHAR(200),
    text VARCHAR(15000),
    is_parsed CHAR(1),

    PRIMARY KEY (article_id, section_id),

    CONSTRAINT fk_section_article
        FOREIGN KEY (article_id)
        REFERENCES article(id)
        ON DELETE CASCADE
);

CREATE INDEX idx_article_section_article_id
    ON article_section(article_id);


CREATE TABLE article_section_format (
    id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    article_id INTEGER NOT NULL,
    section_id INTEGER NOT NULL,
    start_pos INTEGER NOT NULL,
    end_pos INTEGER NOT NULL,
    format VARCHAR(64) NOT NULL,
    link VARCHAR(1000),

    CONSTRAINT fk_format_article
        FOREIGN KEY (article_id)
        REFERENCES article(id)
        ON DELETE CASCADE,

    CONSTRAINT fk_format_section
        FOREIGN KEY (article_id, section_id)
        REFERENCES article_section(article_id, section_id)
        ON DELETE CASCADE
);

CREATE INDEX idx_format_article_id
    ON article_section_format(article_id);

CREATE TABLE article_section_ext_text (
    article_id INTEGER NOT NULL,
    section_id INTEGER NOT NULL,
    count_id INTEGER NOT NULL,
    text VARCHAR(15000),

    PRIMARY KEY (article_id, section_id, count_id),

    CONSTRAINT fk_ext_article
        FOREIGN KEY (article_id)
        REFERENCES article(id)
        ON DELETE CASCADE,

    CONSTRAINT fk_ext_section
        FOREIGN KEY (article_id, section_id)
        REFERENCES article_section(article_id, section_id)
        ON DELETE CASCADE
);

CREATE INDEX idx_ext_article_id
    ON article_section_ext_text(article_id);

CREATE TABLE parsed_event (
    id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    article_id INTEGER NOT NULL,
    section_id INTEGER NOT NULL,
    start_date BIGINT,
    end_date BIGINT,
    parse_status INTEGER,
    date_text VARCHAR(200),
    start_pos INTEGER NOT NULL,
    end_pos INTEGER NOT NULL,
    display_text VARCHAR(500),

    CONSTRAINT fk_event_article
        FOREIGN KEY (article_id)
        REFERENCES article(id)
        ON DELETE CASCADE,

    CONSTRAINT fk_event_section
        FOREIGN KEY (article_id, section_id)
        REFERENCES article_section(article_id, section_id)
        ON DELETE CASCADE
);

CREATE INDEX idx_event_article_id
    ON parsed_event(article_id);

