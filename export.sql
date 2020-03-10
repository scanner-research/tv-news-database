-- I think the algorithm if expressed as sql on a per video basis is:
-- 1) select face-genders by distinct face_id (use precedence, nonbinary first then knn-gender)
-- 2) left outer join with face-identities
-- 3) select distinct again by face_id, taking rows with largest score on face-identity (also, not dropping any of the null ones)
-- 4) annotate with a host bit for all hosts of that video (channel hosts + show hosts)
-- 5) join with faces

COPY (

-- Get the most confident identity for each face (90 seconds)
WITH identities AS (
	SELECT face_identity.*
	FROM face_identity
	RIGHT JOIN (SELECT face_id, MAX(score) AS max_score FROM face_identity GROUP BY face_id) AS t
	ON face_identity.face_id = t.face_id AND face_identity.score = t.max_score),


-- Get the gender for each face, taking nonbinary (3) if it exists (7 minutes)
	genders AS (
	SELECT face_id, MAX(gender_id) AS gender_id
	FROM face_gender
	GROUP BY face_id),

-- Get all hosts (3 milliseconds)
	hosts AS (
		SELECT identity_id FROM channel_host
		UNION ALL 
		SELECT identity_id FROM canonical_show_host
	)

-- Join face with gender and identity
SELECT face.*, genders.gender_id, identities.*, hosts.identity_id IS NOT NULL AS is_host
FROM face
LEFT JOIN identities ON identities.face_id = face.id
LEFT JOIN genders ON genders.face_id = face.id
LEFT JOIN hosts ON hosts.identity_id = identities.identity_id

) TO '/newdisk/result.csv';
