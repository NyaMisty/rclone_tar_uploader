# rclone_tar_uploader

- Download URLs
- Store into TAR
- Upload to Rclone
- **FULLY STREAMING**

## Usage

### Full Streaming
**Full Streaming ONLY supports remotes that allowing Rcat without specifying size.**

Here's an incomplete backend list:
- Google Drive
- Dropbox
- local
- ...

This tool will only works well with backend above.

### Syntax

```
./picdown [urllist] [download_worker_count] [upload_worker_count] [max_single_file_size] [remote_path]
```

### Example

```
./picdown item_urls_1.txt 700 10 test_remote:test/item_1_
```

will download into tar: `test_remote:test/item_1_1.tar` `test_remote:test/item_1_2.tar` ...