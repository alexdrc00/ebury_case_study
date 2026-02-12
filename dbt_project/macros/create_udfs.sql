{% macro create_udfs() %}
	-- Enhanced DATE Cast
	CREATE OR REPLACE FUNCTION try_cast_date(p_date_string TEXT, p_format TEXT)
	RETURNS DATE AS $$
	DECLARE
		v_result DATE;
	BEGIN
		IF p_date_string IS NULL OR TRIM(p_date_string) = '' THEN
			RETURN NULL;
		END IF;

		BEGIN
			v_result := TO_DATE(p_date_string, p_format);

			-- Additional validation: date should be reasonable
			-- IF v_result >= '1900-01-01' AND v_result <= CURRENT_DATE + INTERVAL '10 years' THEN
			--      RETURN v_result;
			-- END IF;
			RETURN v_result;
		EXCEPTION WHEN others THEN
			RETURN NULL;
		END;
	END;
	$$ LANGUAGE plpgsql IMMUTABLE;


	-- Enhanced NUMERIC Cast

	CREATE OR REPLACE FUNCTION try_cast_numeric(
		p_numeric_string TEXT,
		p_precision INTEGER DEFAULT 10,
		p_scale INTEGER DEFAULT 2
	)
	RETURNS NUMERIC AS $$
	DECLARE
		v_result NUMERIC;
	BEGIN
		IF p_numeric_string IS NULL OR TRIM(p_numeric_string) = '' THEN
			RETURN NULL;
		END IF;

		BEGIN
			EXECUTE format('SELECT %L::NUMERIC(%s,%s)', p_in, p_precision, p_scale)
			INTO v_result;
			RETURN v_result;
		EXCEPTION WHEN others THEN
			RETURN NULL;
		END;
	END;
	$$ LANGUAGE plpgsql STABLE;
{% endmacro %}
