CREATE TABLE `chunkOffsets` (
	`id` int unsigned NOT NULL,
    `start` bigint unsigned NOT NULL,
    `end` bigint unsigned NOT NULL,
    PRIMARY KEY (`id`),
    UNIQUE KEY `index_UNIQUE` (`id`) );

CREATE TABLE `wikipageindex` (
  `title` varchar(400) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
  `chunkOffset` int unsigned NOT NULL,
  `articleID` int unsigned NOT NULL,
  --PRIMARY KEY (`articleID`),
  --UNIQUE KEY `title_UNIQUE` (`title`),
  KEY `chunkOffset_FK_idx` (`chunkOffset`),
  CONSTRAINT `chunkOffset_FK` FOREIGN KEY (`chunkOffset`) REFERENCES `chunkoffsets` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE utf8mb4_bin;




CREATE UNIQUE INDEX title_UNIQUE ON wikipageindex (title);
ALTER TABLE wikipageindex ADD PRIMARY KEY (articleID);
CREATE UNIQUE INDEX index_UNIQUE ON chunkOffsets (id);
ALTER TABLE chunkOffsets ADD PRIMARY KEY (id);
ALTER TABLE wikipageindex MODIFY title VARCHAR(400) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;

ALTER TABLE wikipageindex CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;

drop table parsed_event;
drop table article_section_table_row;
drop table article_section_table_cell;
drop table article_section_link;
drop table article_section;
drop table article;
--drop table dump_file;

CREATE TABLE `dump_file` (
    id int unsigned NOT NULL AUTO_INCREMENT,
    file_name varchar(400) NOT NULL,
    tar_info blob(1000) NOT NULL,
    PRIMARY KEY (`id`),
    UNIQUE KEY `df_index_UNIQUE` (`id`)
);

CREATE TABLE `article` (
    `id` int unsigned NOT NULL AUTO_INCREMENT,
    `title` varchar(400) NOT NULL,
    `update` datetime NOT NULL,
    `dump_file_id` int unsigned NOT NULL,
    `dump_idx` int unsigned NOT NULL,
    `url` varchar(600) NOT NULL,
    `redirect` varchar(600),
    `no_dates` boolean,
    PRIMARY KEY (`id`),
    UNIQUE KEY `art_index_UNIQUE` (`id`),
    UNIQUE KEY `title_UNIQUE` (`title`),
    INDEX `dump_file_id` (`dump_file_id`),
     FOREIGN KEY (dump_file_id) REFERENCES dump_file(ID)
);



CREATE TABLE `article_section` (
    `article_id` int unsigned NOT NULL,
    `section_id` int unsigned NOT NULL, -- not auto generated so it can be used to check the number of sections easily
    `tag` varchar(40) NOT NULL,
    `text` varchar(15000),
    PRIMARY KEY (`article_id`,`section_id`),
    INDEX `article_id_idx` (`article_id`),
    FOREIGN KEY (article_id) REFERENCES article(ID)
);


CREATE TABLE `article_section_table_row` (
    `id` int unsigned NOT NULL AUTO_INCREMENT,
    `article_id` int unsigned NOT NULL,
    `section_id` int unsigned NOT NULL,
    `row_idx` int NOT NULL,
    `row_type` varchar(4),
    PRIMARY KEY (`id`),
    INDEX `article_section_id_idx` (`article_id`),
    FOREIGN KEY (article_id) REFERENCES article(ID),
    FOREIGN KEY (section_id) REFERENCES article_section(section_id)
);


CREATE TABLE `article_section_table_cell` (
    `row_id` int unsigned NOT NULL,
    `column_idx` int unsigned NOT NULL,
    `column_span` int unsigned,
    `row_span` int unsigned,
    `text` varchar(10000),
    PRIMARY KEY (`row_id`, `column_idx`),
    INDEX `row_id_idx` (`row_id`),
    FOREIGN KEY (row_id) REFERENCES article_section_table_row(ID)
);



CREATE TABLE `article_section_link` (
    `id` int unsigned NOT NULL AUTO_INCREMENT,
    `article_id` int unsigned NOT NULL,
    `section_id` int unsigned NOT NULL,
    `row_idx` int unsigned,
    `column_idx` int unsigned,
    `start_pos` int NOT NULL, -- the index into the text where the link starts
    `end_pos` int NOT NULL, -- the index into the text where the link ends
    `link` varchar(1000) NOT NULL,
    PRIMARY KEY (`id`),
    INDEX `article_section_id_idx` (`article_id`),
    FOREIGN KEY (article_id) REFERENCES article(ID),
    FOREIGN KEY (section_id) REFERENCES article_section(section_id)
);



-- { 'section': idx, 'rowIdx': rowIdx, 'columnIdx': columnIdx, 'startPos': startPos, 'endPos': endPos, 'dText': dText, 'desc': desc }
CREATE TABLE `parsed_event` (
    id int unsigned NOT NULL AUTO_INCREMENT,
    `article_id` int unsigned NOT NULL,
    `section_id` int unsigned NOT NULL,
    `row_idx` int unsigned,
    `column_idx` int unsigned,
    `start_date` bigint, -- 13 billion * secs in year = 4.1002e+17 seconds since the big bang < 1.8447e+19 (big int unsigned max)
    `end_date` bigint,
    `date_text` varchar(200),
    `start_pos` int NOT NULL, -- the index into the text where the date value starts
    `end_pos` int NOT NULL, -- the index into the text where the data value ends
    `display_text` varchar(500),
    PRIMARY KEY (`id`),
    UNIQUE KEY `pe_index_UNIQUE` (`id`),
    INDEX `article_id_idx` (`article_id`),
    FOREIGN KEY (article_id) REFERENCES article(ID),
    FOREIGN KEY (section_id) REFERENCES article_Section(section_id) 
);