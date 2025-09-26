A simple rust app to merge multiple hashmaps for pda

merge.sh will run on server. every hour, merge all raw hashmaps into a single sqlite

upload.sh will run on a machine with wrangler login. every 10 minutes, rsync from server to machine, and run upload.sh to upload the sqlite to d1.