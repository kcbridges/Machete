using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;

using System.Data;
using System.Data.Entity;
using System.Data.Entity.Core.Objects;
using System.Data.SqlClient;

using Machete.Domain;
using Machete.Domain.Entities;

using Machete.Data;
using Machete.Data.Infrastructure;

using NLog;

namespace Machete.Service
{
    #region public class ReportService (Interface and Constructor)
    public interface IReportService
    {
        IEnumerable<DailySumData> DailySumController(DateTime date);
        IEnumerable<WeeklySumData> WeeklySumController(DateTime beginDate, DateTime endDate);
        IEnumerable<MonthlySumData> MonthlySumController(DateTime beginDate, DateTime endDate);
        IEnumerable<YearSumData> YearlySumController(DateTime beginDate, DateTime endDate);
        IEnumerable<ActivityData> ActivityReportController(DateTime beginDate, DateTime endDate, string reportType);
        IEnumerable<ActivityData> YearlyActController(DateTime beginDate, DateTime endDate);
        IEnumerable<ZipModel> EmployerReportController(DateTime beginDate, DateTime endDate);
        IEnumerable<NewWorkerData> NewWorkerController(DateTime beginDate, DateTime endDate, string reportType);
    }

    public class ReportService : IReportService
    {
        protected readonly IWorkOrderRepository woRepo;
        protected readonly IWorkAssignmentRepository waRepo;
        protected readonly IWorkerRepository wRepo;
        protected readonly IWorkerSigninRepository wsiRepo;
        protected readonly IWorkerRequestRepository wrRepo;
        protected readonly ILookupRepository lookRepo;
        protected readonly ILookupCache lCache;
        protected readonly IEmployerRepository eRepo;
        protected readonly IActivitySigninRepository asRepo;

        public ReportService(IWorkOrderRepository woRepo,
                             IWorkAssignmentRepository waRepo,
                             IWorkerRepository wRepo,
                             IWorkerSigninRepository wsiRepo,
                             IWorkerRequestRepository wrRepo,
                             ILookupRepository lookRepo,
                             ILookupCache lCache,
                             IEmployerRepository eRepo,
                             IActivitySigninRepository asRepo)
        {
            this.woRepo = woRepo;
            this.waRepo = waRepo;
            this.wRepo = wRepo;
            this.wsiRepo = wsiRepo;
            this.wrRepo = wrRepo;
            this.lookRepo = lookRepo;
            this.lCache = lCache;
            this.eRepo = eRepo;
            this.asRepo = asRepo;
        }

    #endregion

        #region BasicFunctions
        /// <summary>
        /// A simple count of worker signins for the given period.
        /// </summary>
        /// <param name="beginDate"></param>
        /// <param name="endDate"></param>
        /// <returns>IQueryable of type ReportUnit </returns>
        public IQueryable<ReportUnit> CountSignins()
        {
            var wsiQ = wsiRepo.GetAllQ();

            return wsiQ
            .GroupBy(g => DbFunctions.TruncateTime(g.dateforsignin))
            .Select(h => new ReportUnit
            {
                date = h.Key,
                count = h.Count()
            })
            .OrderBy(o => o.date);
        }

        public IQueryable<ReportUnit> CountUniqueSignins()
        {
            var wsiQ = wsiRepo.GetAllQ();

            return wsiQ
                .GroupBy(g => g.dwccardnum)
                .Select(h => new
                {
                    info = h.Key.ToString(),
                    date = h.Min(x => x.dateforsignin)
                })
                .GroupBy(g => g.date)
                .Select(y => new ReportUnit
                {
                    date = y.Key,
                    count = y.Count()
                })
                .AsQueryable();

        }
        /// <summary>
        /// A simple count of unduplicated worker signins for the given period.
        /// Note: Casa's policy is that these should reset on beginDate, but that
        /// isn't truly "unduplicated" within the program.
        /// </summary>
        /// <param name="range">IEnumerable<DateTime>, not null</param>
        /// <returns>int</returns>
        public IQueryable<ReportUnit> CountUniqueSignins(IEnumerable<DateTime> range)
        {
            var wsiQ = wsiRepo.GetAllQ();

            return range
                    .GroupJoin(
                        wsiQ
                        .GroupBy(g => g.dwccardnum)
                        .Select(h => new { dwc = h.Key, fsi = h.Min(x => x.dateforsignin) }),
                        x => x,
                        y => DbFunctions.TruncateTime(y.fsi),
                        (x, y) => new ReportUnit
                        {
                            date = x,
                            count = y
                                .GroupBy(g => g.dwc)
                                .Select(z => new
                                {
                                    dwc = z.Key
                                }).Count()
                        })
                    .OrderBy(o => o.date)
                    .AsQueryable();
        }

        /// <summary>
        /// Counts work assignments by date.
        /// </summary>
        /// <param name="beginDate"></param>
        /// <param name="endDate"></param>
        /// <returns></returns>
        public IQueryable<ReportUnit> CountAssignments()
        {
            var waQ = waRepo.GetAllQ();

            return waQ
                .Where(y => y.workerAssignedID.HasValue)
                .GroupBy(gb => DbFunctions.TruncateTime(gb.workOrder.dateTimeofWork))
                .Select(g => new ReportUnit
                {
                    date = g.Key,
                    count = g.Count(),
                    info = ""
                });
        }

        /// <summary>
        /// Counts permanent or temporary work assignments by date.
        /// </summary>
        /// <param name="perm">bool</param>
        /// <returns>IQueryable<ReportUnit></returns>
        public IQueryable<ReportUnit> CountAssignments(bool perm)
        {
            var waQ = waRepo.GetAllQ();

            if(perm) return waQ
                .Where(y => y.workerAssignedID.HasValue
                    && y.workOrder.permanentPlacement)
                .GroupBy(gb => DbFunctions.TruncateTime(gb.workOrder.dateTimeofWork))
                .Select(g => new ReportUnit
                {
                    date = g.Key,
                    count = g.Count(),
                    info = ""
                });
            else return waQ
                .Where(y => y.workerAssignedID.HasValue
                    && !y.workOrder.permanentPlacement)
                .GroupBy(gb => DbFunctions.TruncateTime(gb.workOrder.dateTimeofWork))
                .Select(g => new ReportUnit
                {
                    date = g.Key,
                    count = g.Count(),
                    info = ""
                });
        }

        /// <summary>
        /// Counts unduplicated work assignments for a given time period.
        /// </summary>
        /// <param name="range">IEnumerable<DateTime></param>
        /// <returns></returns>
        public IQueryable<ReportUnit> CountAssignments(IEnumerable<DateTime> range)
        {
            var waQ = waRepo.GetAllQ();

            return range
                .GroupJoin(waQ
                    .Where(w => w.workOrder.dateTimeofWork >= range.First())
                    .GroupBy(g => g.workerAssignedID)
                    .Select(h => new { waID = h.Key, fwa = h.Min(x => x.workOrder.dateTimeofWork) }),
                x => x,
                wa => wa.fwa.Date,
                (x, wa) => new ReportUnit
                {
                    date = x,
                    count = wa
                        .GroupBy(g => g.waID)
                        .Select(z => z.Key)
                        .Count()
                })
                .AsQueryable();
        }


        /// <summary>
        /// CountCancelled()
        /// </summary>
        /// <returns>Returns a count of cancelled orders by date.</returns>
        public IQueryable<ReportUnit> CountCancelled()
        {
            var woQ = woRepo.GetAllQ();

            return woQ.Where(whr => whr.status == WorkOrder.iCancelled)
                .GroupBy(gb => DbFunctions.TruncateTime(gb.dateTimeofWork))
                .Select(g => new ReportUnit
                {
                    date = g.Key,
                    count = g.Count(),
                });
        }

        /// <summary>
        /// Counts by type of dispatch (DWC, HHH, Propio/ea.). Very Casa Latina specific, but these
        /// numbers can also be used by other centers, especially where they have women's programs.
        /// </summary>
        /// <param name="dateRequested">A single DateTime parameter</param>
        /// <returns>IQueryable</returns>
        public IQueryable<TypeOfDispatchModel> CountTypeofDispatch()
        {
            var waQ = waRepo.GetAllQ();
            var dwcId = lCache.getByKeys("worktype", "DWC"); 
            var hhhId = lCache.getByKeys("worktype", "HHH");

            return waQ.Select(x => new
            {
                date = x.workOrder.dateTimeofWork.Date,
                dwc = x.workerAssigned.typeOfWorkID == null ? 0
                        : x.workerAssigned.typeOfWorkID == dwcId ? 1
                        : 0,
                dwcr = x.workerAssigned.typeOfWorkID == null ? 0
                        : x.workerAssigned.typeOfWorkID == dwcId
                        ? x.workOrder.workerRequests == null ? 0
                        : x.workOrder.workerRequests.Select(y => y.WorkerID).Contains(x.workerAssigned.ID) ? 1
                        : 0 : 0,
                hhh = x.workerAssigned.typeOfWorkID == null ? 0
                        : x.workerAssigned.typeOfWorkID == hhhId ? 1
                        : 0,
                hhhr = x.workerAssigned.typeOfWorkID == null ? 0
                        : x.workerAssigned.typeOfWorkID == hhhId
                        ? x.workOrder.workerRequests == null ? 0
                        : x.workOrder.workerRequests.Select(y => y.WorkerID).Contains(x.workerAssigned.ID) ? 1
                        : 0 : 0,
            })
            .GroupBy(g => g.date)
            .Select(x => new TypeOfDispatchModel
            {
                date = x.Key,
                dwcount = x.Sum(y => y.dwc),
                dwcountr = x.Sum(y => y.dwcr),
                hhcount = x.Sum(y => y.hhh),
                hhcountr = x.Sum(y => y.hhhr)
            })
            .OrderBy(o => o.date);
        }

        /// <summary>
        /// Grabs a sum of hours and wages and averages them for a given time period.
        /// </summary>
        /// <param name="beginDate">Start date for the query.</param>
        /// <param name="endDate">End date for the query.</param>
        /// <returns></returns>
        public IQueryable<AverageWageModel> HourlyWageAverage()
        {
            var waQ = waRepo.GetAllQ();
            
            return waQ
                .GroupBy(gb => (DateTime)DbFunctions.TruncateTime(gb.workOrder.dateTimeofWork))
                .Select(x => new AverageWageModel
                {
                    date = x.Key,
                    hours = x.Sum(y => y.hours),
                    wages = Math.Round(x.Sum(y => y.hourlyWage * y.hours), 2, MidpointRounding.AwayFromZero),
                    avg = Math.Round(x.Sum(y => y.hourlyWage * y.hours) / x.Sum(y => y.hours), 2, MidpointRounding.AwayFromZero)
                })
                .OrderBy(o => o.date);
        }

        /// <summary>
        /// Lists jobs in order of occurrence by date.
        /// </summary>
        /// <returns>IQueryable<ReportUnit></returns>
        public IQueryable<ReportUnit> ListJobs()
        {
            var waQ = waRepo.GetAllQ();

            return waQ
                .GroupBy(gb => new {
                    date = DbFunctions.TruncateTime(gb.workOrder.dateTimeofWork),
                    type = gb.skillID
                })
                .Select(group => new ReportUnit
                {
                    date = group.Key.date,
                    count = group.Count(),
                    info = lCache.getByID(group.Key.type).text_EN
                })
                .OrderBy(o => o.date)
                .ThenByDescending(t => t.count);
        }

        /// <summary>
        /// Lists jobs in order of occurrence by date and zip code.
        /// Data can then be aggregated any number of ways; count of
        /// all orders by zip, top jobs for each zip, top jobs for
        /// each zip for each date, etc.
        /// 
        /// Count is Work Assignments. Counts only dispatched WAs.
        /// ZipUnit is a modified version of ReportUnit that includes
        /// a field for zips.
        /// </summary>
        /// <returns>IQueryable<ZipUnit></returns>
        public IQueryable<ZipUnit> ListJobsByZip()
        {
            var waQ = waRepo.GetAllQ();

            return waQ
                .Where(y => y.workerAssignedID.HasValue)
                .GroupBy(gb => new
                {
                    date = DbFunctions.TruncateTime(gb.workOrder.dateTimeofWork),
                    zip = gb.workOrder.zipcode,
                    type = gb.skillID
                })
                .Select(group => new ZipUnit
                {
                    date = group.Key.date,
                    zip = group.Key.zip,
                    count = group.Count(),
                    info = lCache.getByID(group.Key.type).text_EN
                })
                .OrderBy(o => o.date)
                .ThenByDescending(t => t.count);
        }

        /// <summary>
        /// Lists jobs in order of occurrence by date and zip code.
        /// Data can then be aggregated any number of ways; count of
        /// all orders by zip, top jobs for each zip, top jobs for
        /// each zip for each date, etc.
        /// 
        /// Count is Work Orders. Counts only completed Work Orders.
        /// </summary>
        /// <returns>IQueryable<ZipUnit></returns>
        public IQueryable<ReportUnit> ListOrdersByZip()
        {
            var woQ = woRepo.GetAllQ();

            return woQ
                .Where(y => y.status == lCache.getByKeys(LCategory.orderstatus, LOrderStatus.Completed))
                .GroupBy(gb => new
                {
                    date = DbFunctions.TruncateTime(gb.dateTimeofWork),
                    zip = gb.zipcode
                })
                .Select(group => new ReportUnit
                {
                    date = group.Key.date,
                    count = group.Count(),
                    info = group.Key.zip
                })
                .OrderBy(o => o.date)
                .ThenByDescending(t => t.count);
        }

        /// <summary>
        /// Returns all activity signins grouped by day,
        /// then activityName. Members can then aggregate
        /// that information by grouping on "info" (activityName)
        /// and/or specifying a limiting date range.
        /// </summary>
        /// <returns>IQueryable<ReportUnit></returns>
        public IQueryable<ReportUnit> GetActivitySignins()
        {
            var asQ = asRepo.GetAllQ();

            return asQ
                .Where(signin => signin.person != null)
                .GroupBy(gb => new { 
                    date = DbFunctions.TruncateTime(gb.Activity.dateStart), 
                    name = lCache.getByID(gb.Activity.name).text_EN })
                .Select(grouping => new ReportUnit
                {
                    date = grouping.Key.date,
                    info = grouping.Key.name,
                    count = grouping.Count()
                })
                .OrderBy(o => o.date)
                .ThenByDescending(t => t.count);
        }

        /// <summary>
        /// Returns unique activity signins for a given time period;
        /// this method answers the question, "How many individuals
        /// attended (any) class for a given time period?"
        /// 
        /// See third override for limiting on specific activityNames.
        /// </summary>
        /// <returns>IQueryable<ReportUnit></returns>
        public IQueryable<ReportUnit> GetActivitySignins(IEnumerable<DateTime> range)
        {
            var asQ = asRepo.GetAllQ();

            return range.GroupJoin(asQ
    			    .Where(signin => signin.person != null)
	    		    .GroupBy(g => g.personID)
		    	    .Select(sel => new {
			    	    personId = sel.Key,
				        fsi = sel.Min(x => x.dateforsignin)
			        }),
		        x => x,
		        asi => DbFunctions.TruncateTime(asi.fsi),
		        (x, asi) => new ReportUnit
		        {
			        date = x,
			        count = asi.Count()
		        })
                .AsQueryable();
        }

        /// <summary>
        /// Returns unique activity signins for a given time period;
        /// this method answers the question, "How many individuals
        /// attended a specific type of class for a given time period?"
        /// </summary>
        /// <param name="actNameId">The Lookups ID of the ActivityName to filter by.</param>
        /// <returns>IQueryable<ReportUnit></returns>
        public IQueryable<ReportUnit> GetActivitySignins(IEnumerable<DateTime> range, int actNameId)
        {
            var asQ = asRepo.GetAllQ();

            return range.GroupJoin(asQ
                    .Where(signin => signin.person != null
                        && signin.Activity.name == actNameId)
                    .GroupBy(g => g.personID)
                    .Select(sel => new
                    {
                        personId = sel.Key,
                        fsi = sel.Min(x => x.dateforsignin)
                    }),
                x => x,
                asi => DbFunctions.TruncateTime(asi.fsi),
                (x, asi) => new ReportUnit
                {
                    date = x,
                    count = asi.Count()
                })
                .AsQueryable();
        }

        /// <summary>
        /// Returns dates of completion, minutes completed, and the dwccardnum for
        /// all adults attending >x minutes of a particular class for a given time range.
        /// </summary>
        /// <param name="actNameId">The Lookups ID of the activityName to filter by.</param>
        /// <param name="minutesInClass">The amount of time, in minutes, those to be counted must have attended classes of the given activityName type.</param>
        /// <returns>date, dwccardnum, minutesInClass</returns>
        public IQueryable<ReportUnit> GetActivityRockstars(IEnumerable<DateTime> range, int actNameId, int minutesInClass)
        {
            var asQ = asRepo.GetAllQ();

            return range
            .GroupJoin(asQ
                .Where(w => w.Activity.name == actNameId)
                .GroupBy(gb => gb.dwccardnum)
                .Select(asi => new
                {
                    dwc = asi.Key,
                    date = DbFunctions.TruncateTime(asi.Select(sel => sel.dateforsignin).Max()),
                    // Get total minutes of class person has
                    // attended for this activityName:
                    mins = asi
                        .Sum(x => 
                            ((DbFunctions.DiffHours
                                (x.Activity.dateEnd,
                                x.Activity.dateStart)
                                * 60)
                            + DbFunctions.DiffMinutes
                                (x.Activity.dateEnd, 
                                x.Activity.dateStart)))
                })
                .Where(w => w.mins >= minutesInClass),
                x => x,
                minQ => minQ.date,
                (x, minQ) => new ReportUnit {
                    date = x,
                    count = minQ.Count(),
                    info = lCache.getByID(actNameId).text_EN
                })
                .AsQueryable();
        }

        /// <summary>
        /// Returns dates of completion, minutes completed, and the dwccardnum for
        /// all adults attending >x minutes of an array of activityName IDs for a given time range.
        /// </summary>
        /// <param name="actNameId">The Lookups ID of the activityName to filter by.</param>
        /// <param name="minutesInClass">The amount of time, in minutes, those to be counted must have attended classes of the given activityName type.</param>
        /// <returns>date, dwccardnum, minutesInClass</returns>
        public IQueryable<ReportUnit> GetActivityRockstars(IEnumerable<DateTime> range, int[] actNameId, int minutesInClass)
        {
            var asQ = asRepo.GetAllQ();
            string[] naQ = lookRepo.GetManyQ(z => actNameId.Contains(z.ID)).Select(na => na.text_EN + ", ").ToArray();
            string sb = new StringBuilder().Append(naQ).ToString(); // should show all actNames being queried
            sb.TrimEnd(sb[sb.Length - 2]);

            return range
            .GroupJoin(asQ
                .Where(w => actNameId.Contains(w.Activity.name))
                .GroupBy(gb => gb.dwccardnum)
                .Select(asi => new
                {
                    dwc = asi.Key,
                    date = DbFunctions.TruncateTime(asi.Select(sel => sel.dateforsignin).Max()),
                    // Get total minutes of class person has
                    // attended for this activityName:
                    mins = asi
                        .Sum(x =>
                            ((DbFunctions.DiffHours
                                (x.Activity.dateEnd,
                                x.Activity.dateStart)
                                * 60)
                            + DbFunctions.DiffMinutes
                                (x.Activity.dateEnd,
                                x.Activity.dateStart)))
                })
                .Where(w => w.mins >= minutesInClass),
                x => x,
                minQ => minQ.date,
                (x, minQ) => new ReportUnit
                {
                    date = x,
                    count = minQ.Count(),
                    info = sb
                })
                .AsQueryable();
        }

        /// <summary>
        /// Returns a count of new, expired, and still active members by enumerated dates within the given period.
        /// This is a resource intensive query, because it targets specific data with daily granularity; this usage 
        /// can be offset by reducing the interval and increasing the unit of measure.
        /// </summary>
        /// <param name="beginDate"></param>
        /// <param name="endDate"></param>
        /// <param name="unitOfMeasure">The unit of time to measure; "days" or "months".</param>
        /// <param name="interval">The interval of time (7 days, 3 months) as int</param>
        /// <returns>date, enrolledOnDate, expiredOnDate, count</returns>
        public IQueryable<StatusUnit> MemberStatusByDate(IEnumerable<DateTime> range)
        {
            var wQ = wRepo.GetAllQ();

            return range.GroupJoin(wQ,
                x => x,
                y => y.dateOfMembership,
                (x, y) => new
                {
                    date = x,
                    count = wQ.Where(z => z.dateOfMembership < x && z.memberexpirationdate > x).Count(),
                    enrolledOnDate = y.Count()
                }
                )
            .GroupJoin(wQ,
                x => x.date,
                y => y.memberexpirationdate,
                (x, y) => new StatusUnit
                {
                    date = x.date,
                    count = x.count,
                    enrolledOnDate = x.enrolledOnDate,
                    expiredOnDate = y.Count()
                })
            .AsQueryable();
        }

        public IQueryable<MemberDateModel> SingleAdults()
        {
            IQueryable<MemberDateModel> query;

            var wQ = wRepo.GetAllQ();

            query = wQ
                .Where(worker => !worker.livewithchildren && worker.maritalstatus != lCache.getByKeys(LCategory.maritalstatus, LMaritalStatus.Married))
                .Select(x => new MemberDateModel
                {
                    dwcnum = x.dwccardnum,
                    zip = x.Person.zipcode,
                    memDate = x.dateOfMembership,
                    expDate = x.memberexpirationdate
                });

            return query;
        }

        public IQueryable<MemberDateModel> FamilyHouseholds()
        {
            IQueryable<MemberDateModel> query;

            var lQ = lookRepo.GetAllQ();
            var wQ = wRepo.GetAllQ();

            query = wQ
                .Where(worker => worker.livewithchildren && worker.maritalstatus == lCache.getByKeys(LCategory.maritalstatus, LMaritalStatus.Married))
                .Select(x => new MemberDateModel
                {
                    dwcnum = x.dwccardnum,
                    zip = x.Person.zipcode,
                    memDate = x.dateOfMembership,
                    expDate = x.memberexpirationdate
                });

            return query;
        }

        public IQueryable<ReportUnit> ClientProfileHomeless(DateTime beginDate, DateTime endDate)
        {
            IQueryable<ReportUnit> query;


            var wQ = wRepo.GetAllQ();

            query = wQ
                .Where(whr => whr.memberexpirationdate > beginDate && whr.dateOfMembership < endDate)
                .GroupBy(grp => grp.homeless)
                .Select(group => new ReportUnit
                {
                    info = group.Key == null ? "Unknown" : group.Key.ToString(),
                    count = group.Count()
                });

            return query;
        }

        public IQueryable<ReportUnit> ClientProfileHouseholdComposition(DateTime beginDate, DateTime endDate)
        {
            IQueryable<ReportUnit> query;


            var wQ = wRepo.GetAllQ();
            var lQ = lookRepo.GetAllQ();

            query = wQ
                .Where(whr => whr.memberexpirationdate > beginDate && whr.dateOfMembership < endDate)
                .GroupBy(grp => new
                {
                    maritalStatus = grp.maritalstatus,
                    withChildren = grp.livewithchildren
                })
                .Join(lQ,
                    gJoin => gJoin.Key.maritalStatus,
                    lJoin => lJoin.ID,
                    (gJoin, lJoin) => new
                    {
                        maritalStatus = lJoin.text_EN,
                        withChildren = gJoin.Key.withChildren ? "With Children" : "Without Children",
                        count = gJoin.Count()
                    })
                .Select(glJoin => new ReportUnit
                {
                    info = glJoin.maritalStatus + ", " + glJoin.withChildren,
                    count = glJoin.count
                });

            return query;
        }

        public IQueryable<ReportUnit> ClientProfileIncome(DateTime beginDate, DateTime endDate)
        {
            IQueryable<ReportUnit> query;


            var wQ = wRepo.GetAllQ();
            var lQ = lookRepo.GetAllQ();

            query = wQ
                .Where(whr => whr.memberexpirationdate > beginDate && whr.dateOfMembership < endDate)
                .Join(lQ,
                    wJoin => wJoin.incomeID,
                    lJoin => lJoin.ID,
                    (gJoin, lJoin) => new
                    {
                        incomeLevel = lJoin.text_EN == "Less than $15,000" ? "Very low (< 30% median)" :
                                        lJoin.text_EN == "Between $15,000 and $25,000" ? "Moderate (> 50% median)" :
                                        lJoin.text_EN != "unknown" ? "Above moderate (> 80% median)" :
                                        "Unknown"
                    })
                .GroupBy(grp => grp.incomeLevel)
                .Select(group => new ReportUnit
                {
                    info = group.Key,
                    count = group.Count()
                })
                .OrderBy(ob => ob.count);

            return query;
        }

        public IQueryable<ReportUnit> ClientProfileWorkerAge(DateTime beginDate, DateTime endDate)
        {
            IQueryable<ReportUnit> query;


            var wQ = wRepo.GetAllQ();

            query = wQ
                .Where(whr => whr.memberexpirationdate > beginDate && whr.dateOfMembership < endDate)
                .Select(worker => new
                {
                    age = (new DateTime(1753, 1, 1) + (DateTime.Now - worker.dateOfBirth)).Year - 1753,
                    dob = worker.dateOfBirth
                })
                .Select(ageO => ageO.dob == new DateTime(1900, 1, 1) ? "Unknown" :
                                ageO.age <= 5 ? "0 to 5 years" :
                                ageO.age <= 12 ? "6 to 12 years" :
                                ageO.age <= 18 ? "13 to 18 years" :
                                ageO.age <= 29 ? "19 to 29 years" :
                                ageO.age <= 45 ? "30 to 45 years" :
                                ageO.age <= 64 ? "46 to 64 years" :
                                ageO.age <= 84 ? "65 to 84 years" :
                                "85+ years")
                .GroupBy(ageCategory => ageCategory)
                .Select(group => new ReportUnit
                {
                    info = group.Key,
                    count = group.Count()
                });

            return query;
			
        }

        public IQueryable<ReportUnit> ClientProfileGender(DateTime beginDate, DateTime endDate)
        {
            IQueryable<ReportUnit> query;


            var wQ = wRepo.GetAllQ();
            var lQ = lookRepo.GetAllQ();

            query = wQ
                .Where(whr => whr.memberexpirationdate > beginDate && whr.dateOfMembership < endDate)
                .GroupBy(worker => worker.Person.gender)
                .Join(lQ,
                    group => group.Key,
                    look => look.ID,
                    (group, look) => new ReportUnit
                    {
                        count = group.Count(),
                        info = look.text_EN
                    });

            return query;
        }

        public IQueryable<ReportUnit> ClientProfileHasDisability(DateTime beginDate, DateTime endDate)
        {
            IQueryable<ReportUnit> query;


            var wQ = wRepo.GetAllQ();

            query = wQ
                .Where(whr => whr.memberexpirationdate > beginDate && whr.dateOfMembership < endDate)
                .GroupBy(worker => worker.disabled)
                .Select(group => new ReportUnit
                {
                    info = group.Key ? "Yes" : "No",
                    count = group.Count()
                });

            return query;
        }

        public IQueryable<ReportUnit> ClientProfileRaceEthnicity(DateTime beginDate, DateTime endDate)
        {
            IQueryable<ReportUnit> query;


            var wQ = wRepo.GetAllQ();
            var lQ = lookRepo.GetAllQ();

            query = wQ
                .Where(whr => whr.memberexpirationdate > beginDate && whr.dateOfMembership < endDate)
                .GroupBy(worker => worker.RaceID)
                .Join(lQ,
                    gJoin => gJoin.Key,
                    lJoin => lJoin.ID,
                    (gJoin, lJoin) => new
                    {
                        race = lJoin.text_EN,
                        count = gJoin.Count()
                    })
                .Select(glJoin => new ReportUnit
                {
                    info = glJoin.race,
                    count = glJoin.count
                });

            return query;
        }

        public IQueryable<ReportUnit> ClientProfileRefugeeImmigrant(DateTime beginDate, DateTime endDate)
        {
            IQueryable<ReportUnit> query;


            var wQ = wRepo.GetAllQ();
            var lQ = lookRepo.GetAllQ();

            query = wQ
                .Where(whr => whr.memberexpirationdate > beginDate && whr.dateOfMembership < endDate)
                .GroupBy(worker => worker.immigrantrefugee)
                .Select(group => new ReportUnit
                {
                    info = group.Key ? "Yes" : "No",
                    count = group.Count()
                });

            return query;
        }

        public IQueryable<ReportUnit> ClientProfileEnglishLevel(DateTime beginDate, DateTime endDate)
        {
            IQueryable<ReportUnit> query;


            var wQ = wRepo.GetAllQ();

            query = wQ
                .Where(whr => whr.memberexpirationdate > beginDate && whr.dateOfMembership < endDate)
                .GroupBy(worker => worker.englishlevelID)
                .Select(group => new ReportUnit
                {
                    info = "English " + group.Key,
                    count = group.Count()
                })
                .OrderBy(ob => ob.info);

            return query;
        }

        #endregion

        #region ReportData

        /// <summary>
        /// Controller for daily summary report.
        /// </summary>
        /// <param name="beginDate"></param>
        /// <param name="endDate"></param>
        /// <returns></returns>
        public IEnumerable<DailySumData> DailySumController(DateTime date)
        {
            IEnumerable<TypeOfDispatchModel> dclCurrent;
            IEnumerable<ReportUnit> dailySignins;
            IEnumerable<ReportUnit> dailyUnique;
            IEnumerable<ReportUnit> dailyAssignments;
            IEnumerable<ReportUnit> dailyCancelled;
            IEnumerable<DailySumData> q;

            var dateRange = GetDateRange(date, date);
            dclCurrent = CountTypeofDispatch().ToList();
            dailySignins = CountSignins().ToList();
            dailyUnique = CountUniqueSignins(dateRange).ToList();
            dailyAssignments = CountAssignments(dateRange).ToList();
            dailyCancelled = CountCancelled().ToList();

            q = dclCurrent
                .Select(group => new DailySumData
                {
                    date = group.date,
                    dwcount = group.dwcount,
                    dwcountr = group.dwcountr,
                    hhcount = group.hhcount,
                    hhcountr = group.hhcountr,
                    uniqueSignins = dailyUnique.Where(whr => whr.date == group.date).Select(g => g.count).FirstOrDefault() ?? 0,
                    totalSignins = dailySignins.Where(whr => whr.date == group.date).Select(g => g.count).FirstOrDefault() ?? 0,
                    totalAssignments = dailyAssignments.Where(whr => whr.date == group.date).Select(g => g.count).FirstOrDefault() ?? 0, // should be same as group.count...mayhap could avoid this join
                    cancelledJobs = dailyCancelled.Where(whr => whr.date == group.date).Select(g => g.count).FirstOrDefault() ?? 0
                });

            q = q.OrderBy(p => p.date);

            return q;
        }

        /// <summary>
        /// Controller for weekly summary report.
        /// </summary>
        /// <param name="beginDate"></param>
        /// <param name="endDate"></param>
        /// <returns></returns>
        public IEnumerable<WeeklySumData> WeeklySumController(DateTime beginDate, DateTime endDate)
        {
            IEnumerable<AverageWageModel> weeklyWages;
            IEnumerable<ReportUnit> weeklySignins;
            IEnumerable<ReportUnit> weeklyAssignments;
            IEnumerable<ReportUnit> weeklyJobs;
            IEnumerable<WeeklySumData> q;

            var dateRange = GetDateRange(beginDate, endDate);

            weeklyWages = HourlyWageAverage(beginDate, endDate).ToList();
            weeklySignins = CountSignins(dateRange).ToList();
            weeklyAssignments = CountAssignments(beginDate, endDate).ToList();
            weeklyJobs = ListJobs(beginDate, endDate).ToList();

            q = weeklyWages
                .Select(g => new WeeklySumData
                {
                    dayofweek = g.date.DayOfWeek,
                    date = g.date,
                    totalSignins = weeklySignins.Where(whr => whr.date == g.date).Select(h => h.count).FirstOrDefault() ?? 0,
                    noWeekJobs = weeklyAssignments.Where(whr => whr.date == g.date).Select(h => h.count).FirstOrDefault() ?? 0,
                    weekEstDailyHours = g.hours,
                    weekEstPayment = g.wages,
                    weekHourlyWage = g.avg,
                    topJobs = weeklyJobs.Where(whr => whr.date == g.date)
                });

            q = q.OrderBy(p => p.date);

            return q;
        }

        public IEnumerable<MonthlySumData> MonthlySumController(DateTime beginDate, DateTime endDate)
        {
            IEnumerable<ReportUnit> signins;
            IEnumerable<ReportUnit> unique;
            IEnumerable<ActivityUnit> classes;
            IEnumerable<PlacementUnit> workers;
            IEnumerable<AverageWageModel> average;
            IEnumerable<StatusUnit> status;

            IEnumerable<MonthlySumData> q;

            var dateRange = GetDateRange(beginDate, endDate);

            signins = CountSignins(dateRange).ToList();
            unique = CountUniqueSignins(dateRange).ToList();
            classes = GetActivitySignins(beginDate, endDate).ToList();
            workers = WorkersInJobs(beginDate, endDate).ToList();
            average = HourlyWageAverage(beginDate, endDate).ToList();

            q = dateRange
                .Select(g => new MonthlySumData
                {
                    dateStart = g,
                    dateEnd = g.AddDays(1),
                    totalSignins = signins.Where(w => w.date >= g && w.date < g.AddDays(1)).Select(h => h.count).FirstOrDefault() ?? 0,
                    uniqueSignins = unique.Where(w => w.date >= g && w.date < g.AddDays(1)).Select(h => h.count).FirstOrDefault() ?? 0, //dd
                    dispatched = workers.Where(w => w.date >= g && w.date < g.AddDays(1)).Select(h => h.count).FirstOrDefault() ?? 0,
                    tempDispatched = workers.Where(w => w.date >= g && w.date < g.AddDays(1)).Select(h => h.tempCount).FirstOrDefault() ?? 0, //dd
                    permanentPlacements = workers.Where(w => w.date >= g && w.date < g.AddDays(1)).Select(h => h.permCount).FirstOrDefault() ?? 0, //dd
                    undupDispatched = workers.Where(w => w.date >= g && w.date < g.AddDays(1)).Select(h => h.undupCount).FirstOrDefault() ?? 0, //dd
                    totalHours = average.Where(w => w.date >= g && w.date < g.AddDays(1)).Select(h => h.hours).FirstOrDefault(),
                    totalIncome = average.Where(w => w.date >= g && w.date < g.AddDays(1)).Select(h => h.wages).FirstOrDefault(),
                    avgIncomePerHour = average.Where(w => w.date >= g && w.date < g.AddDays(1)).Select(h => h.avg).FirstOrDefault(),
                });

            return q;
        }

        public IEnumerable<ActivityData> ActivityReportController(DateTime beginDate, DateTime endDate)
        {
            IEnumerable<ReportUnit> eslAssessed;
            IEnumerable<ActivityUnit> getAllClassAttendance;
            IEnumerable<ActivityData> q;
            IEnumerable<DateTime> getDates;

            getDates = Enumerable.Range(0, 1 + endDate.Subtract(beginDate).Days)
                .Select(offset => beginDate.AddDays(offset))
                .ToArray();

            getAllClassAttendance = GetActivitySignins(beginDate, endDate).ToList();
            eslAssessed = GetActivityRockstars().ToList();
            var safetyTrainees = getAllClassAttendance
                        .Where(whr => whr.activityType == "Health & Safety");
            var skillsTrainees = getAllClassAttendance
                        .Where(whr => whr.activityType == "Skills Training" || whr.activityType == "Leadership Development");
            var basGardenTrainees = getAllClassAttendance.Where(basic => basic.info == "Basic Gardening");
            var advGardenTrainees = getAllClassAttendance.Where(adv => adv.info == "Advanced Gardening");
            var finTrainees = getAllClassAttendance.Where(fin => fin.info == "Financial Education");
            var oshaTrainees = getAllClassAttendance.Where(osha => osha.info.Contains("OSHA"));

            q = getDates
                .Select(g => new ActivityData
                {
                    dateStart = g,
                    safety = safetyTrainees.Where(whr => whr.date == g).Count(),
                    skills = skillsTrainees.Where(whr => whr.date == g).Count(),
                    esl = eslAssessed.Where(whr => whr.date == g).Count(),
                    basGarden = basGardenTrainees.Where(whr => whr.date == g).Count(),
                    advGarden = advGardenTrainees.Where(whr => whr.date == g).Count(),
                    finEd = finTrainees.Where(whr => whr.date == g).Count(),
                    osha = oshaTrainees.Where(whr => whr.date == g).Count(),
                    drilldown = getAllClassAttendance.Where(whr => whr.date == g)
                });

            return q;
        }

        /// <summary>
        /// NewWorkerController returns an IEnumerable containing the counts of single members, new single members, family
        /// members, and new family members. It does not include zip code completeness, which must be called separately.
        /// </summary>
        /// <param name="beginDate"></param>
        /// <param name="endDate"></param>
        /// <returns>date, singleAdults, familyHouseholds, newSingleAdults, newFamilyHouseholds, zipCodeCompleteness</returns>
        public IEnumerable<NewWorkerData> NewWorkerController(DateTime beginDate, DateTime endDate, string reportType)
        {
            IEnumerable<NewWorkerData> q;
            IEnumerable<MemberDateModel> singleAdults;
            IEnumerable<MemberDateModel> familyHouseholds;
            IEnumerable<DateTime> getDates;

            singleAdults = SingleAdults().ToList();
            familyHouseholds = FamilyHouseholds().ToList();

            if (reportType == "weekly" || reportType == "monthly")
            {
                getDates = Enumerable.Range(0, 1 + endDate.Subtract(beginDate).Days)
                   .Select(offset => endDate.AddDays(-offset))
                   .ToArray();

                q = getDates
                    .Select(x => new NewWorkerData
                    {
                        dateStart = x,
                        dateEnd = x.AddDays(1),
                        singleAdults = singleAdults.Where(y => y.expDate >= x && y.memDate < x.AddDays(1)).Count(),
                        familyHouseholds = familyHouseholds.Where(y => y.expDate >= x && y.memDate < x.AddDays(1)).Count(),
                        newSingleAdults = singleAdults.Where(y => y.memDate >= x && y.memDate < x.AddDays(1)).Count(),
                        newFamilyHouseholds = familyHouseholds.Where(y => y.memDate >= x && y.memDate < x.AddDays(1)).Count(),
                        zipCompleteness = singleAdults.Where(y => y.zip != null && y.expDate >= x && y.memDate < x.AddDays(1)).Count()
                                        + familyHouseholds.Where(y => y.zip != null && y.expDate >= x && y.memDate < x.AddDays(1)).Count()
                    });
            }
            else if (reportType == "yearly")
            {
                getDates = Enumerable.Range(1, 4)
                    .Select(offset => endDate.AddMonths(-offset * 3))
                    .ToArray();

                q = getDates
                    .Select(x => new NewWorkerData
                    {
                        dateStart = x.AddDays(1),
                        dateEnd = x.AddMonths(3).AddDays(1),
                        singleAdults = singleAdults.Where(y => y.expDate >= x && y.memDate < x.AddMonths(3).AddDays(1)).Count(),
                        familyHouseholds = familyHouseholds.Where(y => y.expDate >= x && y.memDate < x.AddMonths(3).AddDays(1)).Count(),
                        newSingleAdults = singleAdults.Where(y => y.memDate >= x && y.memDate < x.AddMonths(3).AddDays(1)).Count(),
                        newFamilyHouseholds = familyHouseholds.Where(y => y.memDate >= x && y.memDate < x.AddMonths(3).AddDays(1)).Count(),
                        zipCompleteness = singleAdults.Where(y => y.zip != null && y.expDate >= x && y.memDate < x.AddMonths(3).AddDays(1)).Count()
                                        + familyHouseholds.Where(y => y.zip != null && y.expDate >= x && y.memDate < x.AddMonths(3).AddDays(1)).Count()
                    });
            }
            else throw new Exception("Report type must be \"weekly\", \"monthly\" or \"yearly\".");

            return q;
        }

        /// <summary>
        /// Jobs and Zip Codes controller. The jobs and zip codes report was
        /// initially requested by Mountain View and centers can see what their 
        /// orders are and where they're coming from.
        /// </summary>
        /// <param name="beginDate"></param>
        /// <param name="endDate"></param>
        /// <returns></returns>
        public IEnumerable<ZipModel> EmployerReportController(DateTime beginDate, DateTime endDate)
        {
            IEnumerable<ZipModel> topZips;
            topZips = ListOrdersByZip(beginDate, endDate).ToList();
            return topZips;
        }

        #region helper methods
        public static IEnumerable<DateTime> GetDateRange(DateTime beginDate, DateTime endDate)
        {
            return Enumerable.Range(0, 1 + endDate.Subtract(beginDate).Days)
                    .Select(offset => beginDate.AddDays(offset));
        }
        #endregion 

    }
    #endregion

    #region Report Models
    // The standalone models in this section are mostly at the bottom. Models that serve
    // as units of larger reports are at the top. Most of the models extend from a class
    // called "ReportUnit" and the class and its derivatives are in the middle of this
    // section.

    public class AverageWageModel
    {
        public DateTime date { get; set; }
        public int hours { get; set; }
        public double wages { get; set; }
        public double avg { get; set; }
    }

    public class MemberDateModel
    {
        public int dwcnum { get; set; }
        public string zip { get; set; }
        public DateTime memDate { get; set; }
        public DateTime expDate { get; set; }
    }

    public class TypeOfDispatchModel
    {
        public DateTime date { get; set; }
        public int dwcount { get; set; }
        public int dwcountr { get; set; }
        public int hhcount { get; set; }
        public int hhcountr { get; set; }
    }

    public class StatusUnit : ReportUnit
    {
        public int? expiredOnDate { get; set; }
        public int? enrolledOnDate { get; set; }
    }

    /// <summary>
    /// This gets a little confusing because of all the
    /// different "Zip" models. ZipUnit is a direct
    /// extension of ReportUnit and includes a space
    /// for a zip code string.
    /// </summary>
    public class ZipUnit : ReportUnit
    {
        public string zip { get; set; }
    }

    public class ActivityUnit : ReportUnit
    {
        public string activityType { get; set; }
    }

    /// <summary>
    /// This is the basic unit of the Report service layer.
    /// All of the values are nullable to make the unit as
    /// extensible as possible.
    /// </summary>
    public class ReportUnit
    {
        public DateTime? date { get; set; }
        public int? count { get; set; }
        public string info { get; set; }
    }

    public class DailySumData : TypeOfDispatchModel
    {
        public int totalSignins { get; set; }
        public int uniqueSignins { get; set; }
        public int cancelledJobs { get; set; }
        public int totalAssignments { get; set; }
    }
    /// <summary>
    /// A class to contain the data for the Weekly Report
    /// int totalSignins, int noWeekJobs, int weekJobsSector, int
    /// weekEstDailyHours, double weekEstPayment, double weekHourlyWage
    /// </summary>
    public class WeeklySumData
    {
        public DayOfWeek dayofweek { get; set; }
        public DateTime date { get; set; }
        public int totalSignins { get; set; }
        public int noWeekJobs { get; set; }
        public int weekEstDailyHours { get; set; }
        public double weekEstPayment { get; set; }
        public double weekHourlyWage { get; set; }
        public IEnumerable<ReportUnit> topJobs { get; set; }
    }

    /// <summary>
    /// A class containing all of the data for the Monthly Report with Details
    /// DateTime date, int totalDWCSignins, int totalHHHSignins
    /// dispatchedDWCSignins, int dispatchedHHHSignins
    /// </summary>
    public class MonthlySumData
    {
        public DateTime dateStart { get; set; }
        public DateTime dateEnd { get; set; }
        public int totalSignins { get; set; }
        public int uniqueSignins { get; set; }
        public int dispatched { get; set; }
        public int tempDispatched { get; set; }
        public int permanentPlacements { get; set; }
        public int undupDispatched { get; set; }
        public int totalHours { get; set; }
        public double totalIncome { get; set; }
        public double avgIncomePerHour { get; set; }
        public int newlyEnrolled { get; set; }
        public int peopleWhoLeft { get; set; }
    }

    public class YearSumData
    {
        public DateTime dateStart { get; set; }
        public DateTime dateEnd { get; set; }
        public int totalSignins { get; set; }
        public int uniqueSignins { get; set; }
        public int dispatched { get; set; }
        public int tempDispatched { get; set; }
        public int permanentPlacements { get; set; }
        public int undupDispatched { get; set; }
        public int totalHours { get; set; }
        public double totalIncome { get; set; }
        public double avgIncomePerHour { get; set; }
        public int stillHere { get; set; }
        public int newlyEnrolled { get; set; }
        public int peopleWhoLeft { get; set; }
        public int peopleWhoWentToClass { get; set; }
    }

    public class ZipModel
    {
        public string zips { get; set; }
        public int jobs { get; set; }
        public int emps { get; set; }
        public IEnumerable<ReportUnit> skills { get; set; }
    }

    public class NewWorkerData
    {
        public DateTime? dateStart { get; set; }
        public DateTime? dateEnd { get; set; }
        public int singleAdults { get; set; }
        public int familyHouseholds { get; set; }
        public int newSingleAdults { get; set; }
        public int newFamilyHouseholds { get; set; }
        public int zipCompleteness { get; set; }
    }
    
    public class ActivityData
    {
        public DateTime? dateStart { get; set; }
        public DateTime? dateEnd { get; set; }
        public int safety { get; set; }
        public int skills { get; set; }
        public int esl { get; set; }
        public int basGarden { get; set; }
        public int advGarden { get; set; }
        public int finEd { get; set; }
        public int osha { get; set; }
        public IEnumerable<ActivityUnit> drilldown { get; set; }
    }

    #endregion
}