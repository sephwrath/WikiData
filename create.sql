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
drop table article_section;
drop table article;
drop table dump_file;

CREATE TABLE `dump_file` (
    id int unsigned NOT NULL AUTO_INCREMENT,
    file_name varchar(400) NOT NULL,
    PRIMARY KEY (`id`),
    UNIQUE KEY `df_index_UNIQUE` (`id`)
);

CREATE TABLE `article` (
    `id` int unsigned NOT NULL AUTO_INCREMENT,
    `title` varchar(400) NOT NULL,
    `update` datetime NOT NULL,
    `dump_file_id` int unsigned NOT NULL,
    `dump_idx` int unsigned NOT NULL,
    PRIMARY KEY (`id`),
    UNIQUE KEY `art_index_UNIQUE` (`id`),
    UNIQUE KEY `title_UNIQUE` (`title`),
     FOREIGN KEY (dump_file_id) REFERENCES dump_file(ID)
);

CREATE TABLE `article_section` (
    `id` int unsigned NOT NULL AUTO_INCREMENT,
    `article_id` int unsigned NOT NULL,
    `tag` varchar(400) NOT NULL,
    `text` varchar(15000),
    PRIMARY KEY (`id`),
    UNIQUE KEY `as_index_UNIQUE` (`id`),
    FOREIGN KEY (article_id) REFERENCES article(ID)
);

CREATE TABLE `parsed_event` (
    id int unsigned NOT NULL AUTO_INCREMENT,
    `article_id` int unsigned NOT NULL,
    `section_id` int unsigned NOT NULL,
    `start` bigint NOT NULL, -- 13 billion * secs in year = 4.1002e+17 seconds since the big bang < 1.8447e+19 (big int unsigned max)
    `end` bigint NOT NULL,
    `date_text` varchar(150),
    `st_index` int NOT NULL, -- the index into the text where the date value starts
    `ed_index` int NOT NULL, -- the index into the text where the data value ends
    `display_text` varchar(500) NOT NULL,
    PRIMARY KEY (`id`),
    UNIQUE KEY `pe_index_UNIQUE` (`id`),
    FOREIGN KEY (article_id) REFERENCES article(ID),
    FOREIGN KEY (section_id) REFERENCES article_Section(ID) 
);