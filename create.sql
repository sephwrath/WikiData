

drop table parsed_event;
drop table article_section_ext_text;
drop table article_section_link;
drop table article_section;
drop table article;
--drop table dump_file;

CREATE TABLE `dump_file` (
    id int unsigned NOT NULL AUTO_INCREMENT,
    file_name varchar(400) NOT NULL,
    tar_info blob(1000) NOT NULL,
    offset bigint unsigned NOT NULL,
    offset_data bigint unsigned NOT NULL,
    PRIMARY KEY (`id`),
    UNIQUE KEY `df_index_UNIQUE` (`id`)
);

CREATE TABLE `article` (
    `id` int unsigned NOT NULL AUTO_INCREMENT,
    `title` varchar(400) NOT NULL,
    `title_srch` varchar(400) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci,
    `description` varchar(1000) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci,
    `update` datetime NOT NULL,
    `dump_file_id` int unsigned,
    `dump_idx` int unsigned,
    `url` varchar(600) NOT NULL,
    `redirect` varchar(600),
    `no_dates` boolean,
    `wiki_update_ts` timestamp,
    `err` varchar(30), -- either UP_TO_DATE, NEW_DUMP, PARSE_ERROR
    PRIMARY KEY (`id`),
    UNIQUE KEY `art_index_UNIQUE` (`id`),
    UNIQUE KEY `title_UNIQUE` (`title`),
    INDEX `dump_file_id` (`dump_file_id`),
     FOREIGN KEY (dump_file_id) REFERENCES dump_file(ID)
);

ALTER TABLE test ADD FULLTEXT INDEX `fulltext`(title_srch);

CREATE TABLE `article_section` (
    `article_id` int unsigned NOT NULL,
    `section_id` int unsigned NOT NULL, -- not auto generated so it can be used to check the number of sections easily
    `ext_text_count` int unsigned,
    `parent_section_id` int unsigned,
    `row_idx` int unsigned,
    `column_idx` int unsigned,
    `row_span` int unsigned,
    `column_span` int unsigned,
    `tag` varchar(40) NOT NULL,
    `format` varchar(200),
    `text` varchar(15000),
    `is_parsed` char(1),
    PRIMARY KEY (`article_id`,`section_id`),
    INDEX `article_id_idx` (`article_id`),
    FOREIGN KEY (article_id) REFERENCES article(ID)
);

CREATE TABLE `article_section_link` (
    `id` int unsigned NOT NULL AUTO_INCREMENT,
    `article_id` int unsigned NOT NULL,
    `section_id` int unsigned NOT NULL,
    `start_pos` int NOT NULL, -- the index into the text where the link starts
    `end_pos` int NOT NULL, -- the index into the text where the link ends
    `link` varchar(1000) NOT NULL,
    PRIMARY KEY (`id`),
    INDEX `article_section_link_id_idx` (`article_id`),
    FOREIGN KEY (article_id) REFERENCES article(ID),
    FOREIGN KEY (article_id, section_id) REFERENCES article_section(article_id, `section_id`)
);

-- table to contain any extended text that won't fit in cells or article sections
CREATE TABLE `article_section_ext_text` (
    `article_id` int unsigned NOT NULL,
    `section_id` int unsigned NOT NULL,
    `count_id` int unsigned,
    `text` varchar(15000),
    PRIMARY KEY (`article_id`,`section_id`, `count_id`),
    INDEX `article_section_id_idx` (`article_id`),
    FOREIGN KEY (article_id) REFERENCES article(ID),
    FOREIGN KEY (article_id, section_id) REFERENCES article_section(article_id, section_id)
);



-- { 'section': idx, 'rowIdx': rowIdx, 'columnIdx': columnIdx, 'startPos': startPos, 'endPos': endPos, 'dText': dText, 'desc': desc }
CREATE TABLE `parsed_event` (
    id int unsigned NOT NULL AUTO_INCREMENT,
    `article_id` int unsigned NOT NULL,
    `section_id` int unsigned NOT NULL,
    `start_date` bigint, -- 13 billion * secs in year = 4.1002e+17 seconds since the big bang < 1.8447e+19 (big int unsigned max)
    `end_date` bigint,
    `parse_status` int,
    `date_text` varchar(200),
    `start_pos` int NOT NULL, -- the index into the text where the date value starts
    `end_pos` int NOT NULL, -- the index into the text where the data value ends
    `display_text` varchar(500),
    PRIMARY KEY (`id`),
    UNIQUE KEY `pe_index_UNIQUE` (`id`),
    INDEX `article_id_idx` (`article_id`),
    FOREIGN KEY (article_id) REFERENCES article(ID),
    FOREIGN KEY (article_id, section_id) REFERENCES article_Section(article_id, section_id) 
);




--SELECT `table_name`, table_rows, ROUND((data_length + index_length) / 1024 / 1024, 1) "DB Size in MB" 
--FROM information_schema.tables;