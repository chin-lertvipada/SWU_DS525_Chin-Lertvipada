CREATE TABLE public.Repo (
                repo_id BIGINT NOT NULL,
                repo_name VARCHAR(100) NOT NULL,
                repo_url VARCHAR(150) NOT NULL,
                PRIMARY KEY (repo_id)
);


CREATE TABLE public.Org (
                org_id BIGINT NOT NULL,
                org_login VARCHAR(50) NOT NULL,
                org_gravatar_id VARCHAR(50),
                org_url VARCHAR(100) NOT NULL,
                org_avatar_url VARCHAR(100) NOT NULL,
                PRIMARY KEY (org_id)
);


CREATE TABLE public.Actor (
                actor_id BIGINT NOT NULL,
                actor_login VARCHAR(50) NOT NULL,
                actor_display_login VARCHAR(50) NOT NULL,
                actor_gravatar_id VARCHAR(50),
                actor_url VARCHAR(100) NOT NULL,
                actor_avatar_url VARCHAR(100) NOT NULL,
                PRIMARY KEY (actor_id)
);


CREATE TABLE public.Event (
                event_id VARCHAR(20) NOT NULL,
                event_type VARCHAR(50) NOT NULL,
                event_public BOOLEAN NOT NULL,
                event_created_at TIMESTAMP NOT NULL,
                event_repo_id BIGINT NOT NULL,
                event_actor_id BIGINT NOT NULL,
                event_org_id BIGINT,
                PRIMARY KEY (event_id),
                FOREIGN KEY (event_repo_id)  REFERENCES Repo  (repo_id),
                FOREIGN KEY (event_actor_id) REFERENCES Actor (actor_id),
                FOREIGN KEY (event_org_id)   REFERENCES Org   (org_id)
);