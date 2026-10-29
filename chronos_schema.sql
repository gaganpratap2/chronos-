--
-- PostgreSQL database dump
--

\restrict lAGlM4yLl72PGt3NUwBruCLSqFdzjopf7W6i96WrcRUlHgFetFu4EQJMGnyLlNH

-- Dumped from database version 18.6
-- Dumped by pg_dump version 18.6

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET transaction_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: job_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.job_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: jobs_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.jobs_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: jobs; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.jobs (
    id integer DEFAULT nextval('public.jobs_id_seq'::regclass) NOT NULL,
    name text NOT NULL,
    status text NOT NULL,
    retry_count integer DEFAULT 0 NOT NULL,
    max_retries integer DEFAULT 3 NOT NULL,
    next_retry_at timestamp with time zone,
    worker_id text,
    lease_until timestamp with time zone,
    fencing_token bigint DEFAULT 0 NOT NULL,
    run_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: recurring_schedules; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.recurring_schedules (
    id bigint NOT NULL,
    name text NOT NULL,
    interval_seconds bigint NOT NULL,
    next_run_at timestamp with time zone NOT NULL,
    enabled boolean DEFAULT true NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    catch_up_policy text DEFAULT 'skip'::text NOT NULL,
    max_catch_up integer DEFAULT 100 NOT NULL,
    schedule_type text DEFAULT 'interval'::text NOT NULL,
    cron_expression text,
    timezone text,
    CONSTRAINT recurring_schedules_catch_up_policy_check CHECK ((catch_up_policy = ANY (ARRAY['fire_all'::text, 'fire_once'::text, 'skip'::text]))),
    CONSTRAINT recurring_schedules_cron_fields_check CHECK ((((schedule_type = 'interval'::text) AND (cron_expression IS NULL) AND (timezone IS NULL)) OR ((schedule_type = 'cron'::text) AND (cron_expression IS NOT NULL) AND (timezone IS NOT NULL)))),
    CONSTRAINT recurring_schedules_interval_seconds_check CHECK ((((schedule_type = 'interval'::text) AND (interval_seconds > 0)) OR ((schedule_type = 'cron'::text) AND (interval_seconds = 0)))),
    CONSTRAINT recurring_schedules_max_catch_up_check CHECK ((max_catch_up > 0)),
    CONSTRAINT recurring_schedules_type_check CHECK ((schedule_type = ANY (ARRAY['interval'::text, 'cron'::text])))
);


--
-- Name: recurring_schedules_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.recurring_schedules_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: recurring_schedules_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.recurring_schedules_id_seq OWNED BY public.recurring_schedules.id;


--
-- Name: task_dependencies; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.task_dependencies (
    id bigint NOT NULL,
    task_id bigint NOT NULL,
    depends_on_task_id bigint NOT NULL,
    CONSTRAINT no_self_dependency CHECK ((task_id <> depends_on_task_id))
);


--
-- Name: task_dependencies_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.task_dependencies_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: task_dependencies_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.task_dependencies_id_seq OWNED BY public.task_dependencies.id;


--
-- Name: workflow_tasks; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.workflow_tasks (
    id bigint NOT NULL,
    workflow_id bigint NOT NULL,
    name text NOT NULL,
    status text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    job_id bigint
);


--
-- Name: workflow_tasks_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.workflow_tasks_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: workflow_tasks_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.workflow_tasks_id_seq OWNED BY public.workflow_tasks.id;


--
-- Name: workflows; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.workflows (
    id bigint NOT NULL,
    name text NOT NULL,
    status text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: workflows_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.workflows_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: workflows_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.workflows_id_seq OWNED BY public.workflows.id;


--
-- Name: recurring_schedules id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.recurring_schedules ALTER COLUMN id SET DEFAULT nextval('public.recurring_schedules_id_seq'::regclass);


--
-- Name: task_dependencies id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.task_dependencies ALTER COLUMN id SET DEFAULT nextval('public.task_dependencies_id_seq'::regclass);


--
-- Name: workflow_tasks id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.workflow_tasks ALTER COLUMN id SET DEFAULT nextval('public.workflow_tasks_id_seq'::regclass);


--
-- Name: workflows id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.workflows ALTER COLUMN id SET DEFAULT nextval('public.workflows_id_seq'::regclass);


--
-- Name: jobs jobs_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.jobs
    ADD CONSTRAINT jobs_pkey PRIMARY KEY (id);


--
-- Name: recurring_schedules recurring_schedules_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.recurring_schedules
    ADD CONSTRAINT recurring_schedules_pkey PRIMARY KEY (id);


--
-- Name: task_dependencies task_dependencies_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.task_dependencies
    ADD CONSTRAINT task_dependencies_pkey PRIMARY KEY (id);


--
-- Name: task_dependencies unique_task_dependency; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.task_dependencies
    ADD CONSTRAINT unique_task_dependency UNIQUE (task_id, depends_on_task_id);


--
-- Name: workflow_tasks workflow_tasks_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.workflow_tasks
    ADD CONSTRAINT workflow_tasks_pkey PRIMARY KEY (id);


--
-- Name: workflows workflows_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.workflows
    ADD CONSTRAINT workflows_pkey PRIMARY KEY (id);


--
-- Name: idx_jobs_run_at; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_jobs_run_at ON public.jobs USING btree (run_at);


--
-- Name: idx_recurring_schedules_due; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_recurring_schedules_due ON public.recurring_schedules USING btree (next_run_at) WHERE (enabled = true);


--
-- Name: idx_task_dependencies_depends_on_task_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_task_dependencies_depends_on_task_id ON public.task_dependencies USING btree (depends_on_task_id);


--
-- Name: idx_task_dependencies_task_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_task_dependencies_task_id ON public.task_dependencies USING btree (task_id);


--
-- Name: idx_workflow_tasks_job_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_workflow_tasks_job_id ON public.workflow_tasks USING btree (job_id);


--
-- Name: idx_workflow_tasks_workflow_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_workflow_tasks_workflow_id ON public.workflow_tasks USING btree (workflow_id);


--
-- Name: task_dependencies task_dependencies_depends_on_task_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.task_dependencies
    ADD CONSTRAINT task_dependencies_depends_on_task_id_fkey FOREIGN KEY (depends_on_task_id) REFERENCES public.workflow_tasks(id) ON DELETE CASCADE;


--
-- Name: task_dependencies task_dependencies_task_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.task_dependencies
    ADD CONSTRAINT task_dependencies_task_id_fkey FOREIGN KEY (task_id) REFERENCES public.workflow_tasks(id) ON DELETE CASCADE;


--
-- Name: workflow_tasks workflow_tasks_workflow_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.workflow_tasks
    ADD CONSTRAINT workflow_tasks_workflow_id_fkey FOREIGN KEY (workflow_id) REFERENCES public.workflows(id) ON DELETE CASCADE;


--
-- PostgreSQL database dump complete
--

\unrestrict lAGlM4yLl72PGt3NUwBruCLSqFdzjopf7W6i96WrcRUlHgFetFu4EQJMGnyLlNH

