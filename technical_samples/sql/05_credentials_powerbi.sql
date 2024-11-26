USE EcommerceFootballStore

CREATE LOGIN sqlUser
WITH PASSWORD = 'gitPass145!';


CREATE USER pbiUser
FOR LOGIN sqlUser

GRANT SELECT, INSERT TO pbiUser
