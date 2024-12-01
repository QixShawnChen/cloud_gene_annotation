# gas-framework
An enhanced web framework (based on [Flask](http://flask.pocoo.org/)) for use in the capstone project. Adds robust user authentication (via [Globus Auth](https://docs.globus.org/api/auth)), modular templates, and some simple styling based on [Bootstrap](http://getbootstrap.com/).

Directory contents are as follows:
* `/web` - The GAS web app files
* `/ann` - Annotator files
* `/util` - Utility scripts for notifications, archival, and restoration
* `/aws` - AWS user data files

https://qixshawnchen.mpcs-cc.com/annotations

a. Description of your archive process: 

If a user is not premium user, his file will be archive in 5 min after annotated. The job status of corresponding job will become "ARCHIVED" from "COMPLETED". The corresponding archived files will be sent into glacier. The original result files will be removed from S3 bucket. 


b. Description of your restore process: 

If a free user upgrades to become a premium user, the "ARCHIVED" result files will be sent to restore which will take a long time. During the process of restoration, the job status will become "RESTORING". After the file is restored, the job status will become "RESTORED". The result file will be sent back to S3 bucket with a new name like "restored_test.annot.vcf". The archive_id will be removed from DynamoDB and corresponding archived file will be removed from glacier. 


c. Some other rules:

I add some other rules to make the whole logic more complete. For premium users, their files will not be archived. However, if they unsuscribe premium membership, their files that never haven't been archived will be archived and sent to glacier. To unarchive, they have to be premium user again. For those restored files, they won't be archived again. 


If you find any error in the project especially the web, please contact me as soon as possible. qixshawnchen@uchicago.edu



--------------------------UPDATE MAY 24--------------------------------


For some unkonwn reason related to PostgreSQL, my ELB cannot work normally. I did have finished everything. However, the result I received were always 502 Bad Gateway. 
I have tmux my web on a instance named "qixshawnchen-gas-web-debug". The link became: "https://qixshawnchen.mpcs-cc.com:4433/annotations".
My annotator.py (qixshawnchen-gas-ann) is still using ELB. I have no idea why this happened starting yesterday. Please use https://qixshawnchen.mpcs-cc.com:4433/annotations to access my website.
The webpages using PostgreSQL failed to work while I am using ELB.
