rsync -av pgweb@130.60.23.13:/Users/Shared/mongodump/arraymap_tcga /Users/Shared/mongodump/
for database in arraymap_ga4gh 
  do mongo $database --eval 'db.dropDatabase()'
  mongorestore --db $database /Users/Shared/mongodump/$database/
done
