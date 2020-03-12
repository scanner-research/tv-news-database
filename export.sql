COPY (

WITH identities AS (
	-- Get the most confident identity for each face (10 minutes)
	SELECT face_identity.identity_id, face_identity.score, face_identity.face_id
	FROM face_identity
	INNER JOIN (
		SELECT face_id, MAX(score) AS max_score
		FROM face_identity
		WHERE labeler_id = 609 or labeler_id = 610 -- Only rekognition
		GROUP BY face_id
	) AS t
	ON face_identity.face_id = t.face_id AND face_identity.score = t.max_score
	WHERE labeler_id = 609 or labeler_id = 610 -- Only rekognition
	),


-- Get the gender for each face, with manual relabeling taking precedence (5 minutes)
	genders AS (
	SELECT 
		CASE WHEN manual_gender.face_id IS NULL
			THEN knn_gender.face_id
			ELSE manual_gender.face_id
		END AS face_id,
		CASE WHEN manual_gender.face_id IS NULL
			THEN knn_gender.gender_id
			ELSE manual_gender.gender_id
		END AS gender_id,
		CASE WHEN manual_gender.face_id IS NULL
			THEN knn_gender.score
			ELSE manual_gender.score
		END AS score
	FROM (
		SELECT face_id, gender_id, score
		FROM face_gender
		WHERE labeler_id = 1 -- manual assignment (nonbinary override)
	) manual_gender
	FULL OUTER JOIN (
		SELECT face_id, gender_id, score
		FROM face_gender
		WHERE labeler_id = 551 -- KNN-gender
	) AS knn_gender
	ON manual_gender.face_id = knn_gender.face_id
	),

-- Get all unique hosts (3 milliseconds)
	hosts AS (
	SELECT DISTINCT identity_id FROM (
		SELECT identity_id FROM channel_host
		UNION ALL 
		SELECT identity_id FROM canonical_show_host) t
	)

-- Join face with gender, identity, hosts, and frames (45 minutes)
SELECT
	face.id as face_id,
	frame.video_id,
	frame.number * (1000.0 / video.fps) AS start_ms,
	(face.bbox_y2 - face.bbox_y1) AS height,
	genders.gender_id,
	genders.score AS gender_score,
	identities.identity_id,
	identities.score as identity_score,
	hosts.identity_id IS NOT NULL AS is_host
FROM face
LEFT JOIN identities ON identities.face_id = face.id
LEFT JOIN genders ON genders.face_id = face.id
LEFT JOIN hosts ON hosts.identity_id = identities.identity_id
LEFT JOIN frame ON face.frame_id = frame.id
LEFT JOIN video ON frame.video_id = video.id
WHERE NOT video.is_corrupt AND NOT video.is_duplicate
ORDER BY
	frame.video_id,
	identities.identity_id,
	frame.number,
	face.id

) TO '/newdisk/result.csv' HEADER CSV;
