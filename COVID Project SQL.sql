-- Likelihood of death upon COVID contraction
-- Total Cases vs Total Deaths
SELECT location, date, total_cases, total_deaths, (CONVERT(float, total_deaths)/CONVERT(float, total_cases))*100 AS DeathPercentage
FROM COVIDProject..COVID_Deaths
WHERE location LIKE 'India'
AND continent IS NOT NULL
ORDER BY 1, 2;

-- Percentage of population that contracted COVID in the world
-- Total Deaths vs Population in the world
SELECT location, date, population, total_cases, (CONVERT(float, total_cases)/CONVERT(float, population))*100 AS PercentPopulationInfected
FROM COVIDProject..COVID_Deaths
WHERE continent IS NOT NULL
ORDER BY 1, 2;

-- Countries with highest infection rate compared to population
SELECT location, population, MAX(total_cases) as HighestInfectionCount, MAX((CONVERT(float, total_cases)/CONVERT(float, population)))*100 AS PercentPopulationInfected
FROM COVIDProject..COVID_Deaths
WHERE continent IS NOT NULL
GROUP BY location, population
ORDER BY PercentPopulationInfected DESC;

-- Continents with highest death count per population
SELECT continent, MAX(CAST(total_deaths AS int)) AS totalDeathCount
FROM COVIDProject..COVID_Deaths
WHERE continent IS NOT NULL
GROUP BY continent
ORDER BY totalDeathCount DESC;

-- Countries with highest death count per population
SELECT location, MAX(CAST(total_deaths AS int)) AS totalDeathCount
FROM COVIDProject..COVID_Deaths
WHERE continent IS NOT NULL
GROUP BY location
ORDER BY totalDeathCount DESC;

-- Global numbers per day
SELECT date, SUM(CAST(new_cases as int)) as total_cases, SUM(CAST(new_deaths as int)) as total_deaths, SUM(CAST(new_deaths as int))/NULLIF(SUM(CAST(new_cases as int)), 0) * 100 AS DeathPercentage
FROM COVIDProject..COVID_Deaths
WHERE continent is not null
GROUP BY date
ORDER BY 1, 2;

-- Total global deaths
SELECT SUM(CAST(new_cases as float)) as total_cases, SUM(CAST(new_deaths as float)) as total_deaths, SUM(CAST(new_deaths as float))/SUM(CAST(new_cases as float)) * 100 AS DeathPercentage
FROM COVIDProject..COVID_Deaths
WHERE continent is not null
ORDER BY 1, 2;

-- Joining both datasets (vaccinations and deaths) to view total vaccinations per day
SELECT dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations
, SUM(CAST(vac.new_vaccinations as bigint)) OVER (PARTITION BY dea.location ORDER BY dea.location, dea.Date) as rolling_vaccinations
FROM COVIDProject..COVID_Deaths dea
JOIN COVIDProject..COVID_Vaccinations vac
ON dea.location = vac.location
and dea.date = vac.date
WHERE dea.continent is not null
ORDER BY 2, 3

-- Using CTE
With PopVsVac (continent, location, date, population, new_vaccinations, rolling_vaccinations)
as
(
SELECT dea.continent, dea.location, dea.date, CAST(dea.population as float), vac.new_vaccinations
, SUM(CAST(vac.new_vaccinations as float)) OVER (PARTITION BY dea.location ORDER BY dea.location, dea.Date) as rolling_vaccinations
FROM COVIDProject..COVID_Deaths dea
JOIN COVIDProject..COVID_Vaccinations vac
ON dea.location = vac.location
and dea.date = vac.date
WHERE dea.continent is not null
-- ORDER BY 2, 3
)

SELECT *, (rolling_vaccinations/population)*100 as Pop_vs_Vac
FROM PopVsVac;

-- TEMP TABLE

DROP TABLE if exists #PercentPeopleVaccinated
CREATE TABLE #PercentPeopleVaccinated
(
continent nvarchar(255),
location nvarchar(255),
date datetime,
population numeric,
new_vaccinations numeric,
rolling_vaccinations numeric
)

INSERT INTO #PercentPeopleVaccinated
SELECT dea.continent, dea.location, dea.date, CAST(dea.population as float), vac.new_vaccinations
, SUM(CAST(vac.new_vaccinations as float)) OVER (PARTITION BY dea.location ORDER BY dea.location, dea.Date) as rolling_vaccinations
FROM COVIDProject..COVID_Deaths dea
JOIN COVIDProject..COVID_Vaccinations vac
ON dea.location = vac.location
and dea.date = vac.date
WHERE dea.continent is not null
-- ORDER BY 2, 3

SELECT *, (rolling_vaccinations/population)*100 as Pop_vs_Vac
FROM #PercentPeopleVaccinated;

-- Creating view to store data for visualization
CREATE VIEW PercentPeopleVaccinated as
SELECT dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations
, SUM(CAST(vac.new_vaccinations as float)) OVER (PARTITION BY dea.location ORDER BY dea.location, dea.Date) as rolling_vaccinations
FROM COVIDProject..COVID_Deaths dea
JOIN COVIDProject..COVID_Vaccinations vac
ON dea.location = vac.location
and dea.date = vac.date
WHERE dea.continent is not null
