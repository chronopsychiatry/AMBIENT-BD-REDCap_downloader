def generate_config():
    with open('REDCap_downloader.properties', 'w') as f:
        config = ('[DEFAULT]\n'
                  '# Files and directories\n'
                  'token-file=.\\.auth\\redcap_token.txt\n'
                  'download-dir=.\\REDCap_data\n'
                  '# Report ID to download from REDCap\n'
                  'report-id=159\n'
                  '# Log level: INFO (default) or DEBUG\n'
                  'log-level=INFO'
                  )
        f.write(config)
