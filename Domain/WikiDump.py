import os
import tarfile
from typing import Iterator, List

from WikiData.Domain.DataClasses import Article, DumpFile, DumpProgress

class WikiDump:
    def __init__(self, dump_file: DumpFile, progress: DumpProgress):
        self.dump_file = dump_file
        self.progress = progress

    def file_name_generator(self) -> Iterator[tarfile.TarInfo]:
        with tarfile.open(self.file_path, mode="r:gz") as tf:
            while True:
                member = tf.next()
                if member is None:
                    break

                if not member.isfile():
                    continue

                yield member

    def file_gnerator(self, json_save_path: str, file_start_offset: int) -> Iterator[bytes]:
        with open(json_save_path, "r", encoding=tarfile.ENCODING) as file_output:
            for (idx, line) in enumerate(file_output):
                if (idx <= file_start_offset):
                    continue
                yield line.encode(tarfile.ENCODING)

    def tar_file_line_generator(self, file_path: str, dumps: List[DumpFile], file_start_offset: int, first_file_id: int) -> Iterator[bytes]:
        with tarfile.open(file_path, mode="r:gz") as tf:
            for file_rec in dumps:
                # load the member info from the colum as using get members requires reading the whole tarfile
                member = tarfile.TarInfo.frombuf(file_rec.tar_info, tarfile.ENCODING, 'surrogateescape')
                # the offset and offset_data are not saved by TarInfo.tobuf() or restored by TarInfo.frombuf() so they need to be set manually
                # this seems to be a bug in the Tare file library - TODO submit a bug report
                member.offset = file_rec.offset
                member.offset_data = file_rec.offset_data

                # reset the line offset for subsequent files
                if file_rec.id > first_file_id:
                    file_start_offset = -1

                with tf.extractfile(member) as file_input:

                    # loop through each of the articles in the files
                    for (idx, line) in enumerate(file_input):
                         # skip until we catch up to the last inserted article
                        if (idx <= file_start_offset):
                            continue
                        yield  idx, line, file_rec.id

    def tar_file_article(self, file_path: str, dump: DumpFile, article: Article) -> tuple[int, int]:
        with tarfile.open(file_path, mode="r:gz") as tf:
                # load the member info from the colum as using get members requires reading the whole tarfile
                member = tarfile.TarInfo.frombuf(dump.tar_info, tarfile.ENCODING, 'surrogateescape')
                # the offset and offset_data are not saved by TarInfo.tobuf() or restored by TarInfo.frombuf() so they need to be set manually
                # this seems to be a bug in the Tare file library - TODO submit a bug report
                member.offset = dump.offset
                member.offset_data = dump.offset_data

                with tf.extractfile(member) as file_input:

                    # loop through each of the articles in the files
                    for (idx, line) in enumerate(file_input):
                        # skip until we catch up to the last inserted article
                        if (idx < article.dump_idx):
                            continue
                        return idx, line
        return None, None
    
    def json_file_article(self, json_save_path: str, dump: DumpFile, article: Article):
        with open(json_save_path + article.file_name, "r", encoding=tarfile.ENCODING) as file_output:
            for (idx, line) in enumerate(file_output):
                if (idx < article.dump_idx):
                    continue
                return (idx, line)
        return None, None


    def get_article_count(self):
        prev_run_df = self.get_missing_dump_details()

        with tarfile.open(self.file_path, mode="r:gz") as tf:
            # load the member info from the colum as using get members requires reading the whole tarfile
            
            for file_rec in prev_run_df[1]:
                member = tarfile.TarInfo.frombuf(file_rec.tar_info, tarfile.ENCODING, 'surrogateescape')
                member.offset = file_rec.offset
                member.offset_data = file_rec.offset_data

                with tf.extractfile(member) as file_input:
                    num_lines = sum(1 for _ in file_input)

                print("{} articles in {}".format(num_lines, file_rec.file_name))

    def save_tar_files(self,  save_path : str):
        files = os.listdir(save_path)
        with tarfile.open(self.file_path, mode="r:gz") as tf:
            while True:
                member = tf.next()
                if member is None:
                    break
                if member.isfile():
                    with tf.extractfile(member) as file_input:
                        if member.name not in files:
                            with open(save_path + member.name, "wb") as file_output:
                                file_output.write(file_input.read())

    def create_tar_files(self, save_path : str):
        files = os.listdir(save_path)

        for file in files:
            if file.endswith(".tar.gz"):
                continue
            with tarfile.open(save_path + file + ".tar.gz", mode="w:gz") as tar:
                tar.add(save_path + file, arcname=file)
                tar.close()

            os.remove(save_path + file)