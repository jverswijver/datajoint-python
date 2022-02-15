from itertools import count
import logging
import inspect
import copy
import re
from .settings import config
from .errors import DataJointError
from .fetch import Fetch, Fetch1
from .preview import preview, repr_html
from .condition import (
    AndList,
    Not,
    make_condition,
    assert_join_compatibility,
    extract_column_names,
    PromiscuousOperand,
)
from .declare import CONSTANT_LITERALS

logger = logging.getLogger(__name__)


class QueryExpression:
    """
    QueryExpression implements query operators to derive new entity set from its input.
    A QueryExpression object generates a SELECT statement in SQL.
    QueryExpression operators are restrict, join, proj, aggr, and union.

    A QueryExpression object has a support, a restriction (an AndList), and heading.
    Property `heading` (type dj.Heading) contains information about the attributes.
    It is loaded from the database and updated by proj.

    Property `support` is the list of table names or other QueryExpressions to be joined.

    The restriction is applied first without having access to the attributes generated by the projection.
    Then projection is applied by selecting modifying the heading attribute.

    Application of operators does not always lead to the creation of a subquery.
    A subquery is generated when:
        1. A restriction is applied on any computed or renamed attributes
        2. A projection is applied remapping remapped attributes
        3. Subclasses: Join, Aggregation, and Union have additional specific rules.
    """

    _restriction = None
    _restriction_attributes = None
    _left = []  # list of booleans True for left joins, False for inner joins
    _original_heading = None  # heading before projections

    # subclasses or instantiators must provide values
    _connection = None
    _heading = None
    _support = None

    # If the query will be using distinct
    _distinct = False

    @property
    def connection(self):
        """a dj.Connection object"""
        assert self._connection is not None
        return self._connection

    @property
    def support(self):
        """A list of table names or subqueries to from the FROM clause"""
        assert self._support is not None
        return self._support

    @property
    def heading(self):
        """a dj.Heading object, reflects the effects of the projection operator .proj"""
        return self._heading

    @property
    def original_heading(self):
        """a dj.Heading object reflecting the attributes before projection"""
        return self._original_heading or self.heading

    @property
    def restriction(self):
        """a AndList object of restrictions applied to input to produce the result"""
        if self._restriction is None:
            self._restriction = AndList()
        return self._restriction

    @property
    def restriction_attributes(self):
        """the set of attribute names invoked in the WHERE clause"""
        if self._restriction_attributes is None:
            self._restriction_attributes = set()
        return self._restriction_attributes

    @property
    def primary_key(self):
        return self.heading.primary_key

    _subquery_alias_count = count()  # count for alias names used in the FROM clause

    def from_clause(self):
        support = (
            "(" + src.make_sql() + ") as `$%x`" % next(self._subquery_alias_count)
            if isinstance(src, QueryExpression)
            else src
            for src in self.support
        )
        clause = next(support)
        for s, left in zip(support, self._left):
            clause += " NATURAL{left} JOIN {clause}".format(
                left=" LEFT" if left else "", clause=s
            )
        return clause

    def where_clause(self):
        return (
            ""
            if not self.restriction
            else " WHERE (%s)" % ")AND(".join(str(s) for s in self.restriction)
        )

    def make_sql(self, fields=None):
        """
        Make the SQL SELECT statement.
        :param fields: used to explicitly set the select attributes
        """
        return "SELECT {distinct}{fields} FROM {from_}{where}".format(
            distinct="DISTINCT " if self._distinct else "",
            fields=self.heading.as_sql(fields or self.heading.names),
            from_=self.from_clause(),
            where=self.where_clause(),
        )

    # --------- query operators -----------
    def make_subquery(self):
        """create a new SELECT statement where self is the FROM clause"""
        result = QueryExpression()
        result._connection = self.connection
        result._support = [self]
        result._heading = self.heading.make_subquery_heading()
        return result

    def restrict(self, restriction):
        """
        Produces a new expression with the new restriction applied.
        rel.restrict(restriction)  is equivalent to  rel & restriction.
        rel.restrict(Not(restriction))  is equivalent to  rel - restriction
        The primary key of the result is unaffected.
        Successive restrictions are combined as logical AND:   r & a & b  is equivalent to r & AndList((a, b))
        Any QueryExpression, collection, or sequence other than an AndList are treated as OrLists
        (logical disjunction of conditions)
        Inverse restriction is accomplished by either using the subtraction operator or the Not class.

        The expressions in each row equivalent:

        rel & True                          rel
        rel & False                         the empty entity set
        rel & 'TRUE'                        rel
        rel & 'FALSE'                       the empty entity set
        rel - cond                          rel & Not(cond)
        rel - 'TRUE'                        rel & False
        rel - 'FALSE'                       rel
        rel & AndList((cond1,cond2))        rel & cond1 & cond2
        rel & AndList()                     rel
        rel & [cond1, cond2]                rel & OrList((cond1, cond2))
        rel & []                            rel & False
        rel & None                          rel & False
        rel & any_empty_entity_set          rel & False
        rel - AndList((cond1,cond2))        rel & [Not(cond1), Not(cond2)]
        rel - [cond1, cond2]                rel & Not(cond1) & Not(cond2)
        rel - AndList()                     rel & False
        rel - []                            rel
        rel - None                          rel
        rel - any_empty_entity_set          rel

        When arg is another QueryExpression, the restriction  rel & arg  restricts rel to elements that match at least
        one element in arg (hence arg is treated as an OrList).
        Conversely,  rel - arg  restricts rel to elements that do not match any elements in arg.
        Two elements match when their common attributes have equal values or when they have no common attributes.
        All shared attributes must be in the primary key of either rel or arg or both or an error will be raised.

        QueryExpression.restrict is the only access point that modifies restrictions. All other operators must
        ultimately call restrict()

        :param restriction: a sequence or an array (treated as OR list), another QueryExpression, an SQL condition
        string, or an AndList.
        """
        attributes = set()
        new_condition = make_condition(self, restriction, attributes)
        if new_condition is True:
            return self  # restriction has no effect, return the same object
        # check that all attributes in condition are present in the query
        try:
            raise DataJointError(
                "Attribute `%s` is not found in query."
                % next(attr for attr in attributes if attr not in self.heading.names)
            )
        except StopIteration:
            pass  # all ok
        # If the new condition uses any new attributes, a subquery is required.
        # However, Aggregation's HAVING statement works fine with aliased attributes.
        need_subquery = isinstance(self, Union) or (
            not isinstance(self, Aggregation) and self.heading.new_attributes
        )
        if need_subquery:
            result = self.make_subquery()
        else:
            result = copy.copy(self)
            result._restriction = AndList(
                self.restriction
            )  # copy to preserve the original
        result.restriction.append(new_condition)
        result.restriction_attributes.update(attributes)
        return result

    def restrict_in_place(self, restriction):
        self.__dict__.update(self.restrict(restriction).__dict__)

    def __and__(self, restriction):
        """
        Restriction operator e.g. ``q1 & q2``.
        :return: a restricted copy of the input argument
        See QueryExpression.restrict for more detail.
        """
        return self.restrict(restriction)

    def __xor__(self, restriction):
        """
        Permissive restriction operator ignoring compatibility check  e.g. ``q1 ^ q2``.
        """
        if inspect.isclass(restriction) and issubclass(restriction, QueryExpression):
            restriction = restriction()
        if isinstance(restriction, Not):
            return self.restrict(Not(PromiscuousOperand(restriction.restriction)))
        return self.restrict(PromiscuousOperand(restriction))

    def __sub__(self, restriction):
        """
        Inverted restriction e.g. ``q1 - q2``.
        :return: a restricted copy of the input argument
        See QueryExpression.restrict for more detail.
        """
        return self.restrict(Not(restriction))

    def __neg__(self):
        """
        Convert between restriction and inverted restriction e.g. ``-q1``.
        :return: target restriction
        See QueryExpression.restrict for more detail.
        """
        if isinstance(self, Not):
            return self.restriction
        return Not(self)

    def __mul__(self, other):
        """
        join of query expressions `self` and `other` e.g. ``q1 * q2``.
        """
        return self.join(other)

    def __matmul__(self, other):
        """
        Permissive join of query expressions `self` and `other` ignoring compatibility check
            e.g. ``q1 @ q2``.
        """
        if inspect.isclass(other) and issubclass(other, QueryExpression):
            other = other()  # instantiate
        return self.join(other, semantic_check=False)

    def join(self, other, semantic_check=True, left=False):
        """
        create the joined QueryExpression.
        a * b  is short for A.join(B)
        a @ b  is short for A.join(B, semantic_check=False)
        Additionally, left=True will retain the rows of self, effectively performing a left join.
        """
        # trigger subqueries if joining on renamed attributes
        if isinstance(other, U):
            return other * self
        if inspect.isclass(other) and issubclass(other, QueryExpression):
            other = other()  # instantiate
        if not isinstance(other, QueryExpression):
            raise DataJointError("The argument of join must be a QueryExpression")
        if semantic_check:
            assert_join_compatibility(self, other)
        join_attributes = set(n for n in self.heading.names if n in other.heading.names)
        # needs subquery if self's FROM clause has common attributes with other's FROM clause
        need_subquery1 = need_subquery2 = bool(
            (set(self.original_heading.names) & set(other.original_heading.names))
            - join_attributes
        )
        # need subquery if any of the join attributes are derived
        need_subquery1 = (
            need_subquery1
            or isinstance(self, Aggregation)
            or any(n in self.heading.new_attributes for n in join_attributes)
            or isinstance(self, Union)
        )
        need_subquery2 = (
            need_subquery2
            or isinstance(other, Aggregation)
            or any(n in other.heading.new_attributes for n in join_attributes)
            or isinstance(self, Union)
        )
        if need_subquery1:
            self = self.make_subquery()
        if need_subquery2:
            other = other.make_subquery()
        result = QueryExpression()
        result._connection = self.connection
        result._support = self.support + other.support
        result._left = self._left + [left] + other._left
        result._heading = self.heading.join(other.heading)
        result._restriction = AndList(self.restriction)
        result._restriction.append(other.restriction)
        result._original_heading = self.original_heading.join(other.original_heading)
        assert len(result.support) == len(result._left) + 1
        return result

    def __add__(self, other):
        """union e.g. ``q1 + q2``."""
        return Union.create(self, other)

    def proj(self, *attributes, **named_attributes):
        """
        Projection operator.
        :param attributes:  attributes to be included in the result. (The primary key is already included).
        :param named_attributes: new attributes computed or renamed from existing attributes.
        :return: the projected expression.
        Primary key attributes cannot be excluded but may be renamed.
        If the attribute list contains an Ellipsis ..., then all secondary attributes are included too
        Prefixing an attribute name with a dash '-attr' removes the attribute from the list if present.
        Keyword arguments can be used to rename attributes as in name='attr', duplicate them as in name='(attr)', or
        self.proj(...) or self.proj(Ellipsis) -- include all attributes (return self)
        self.proj() -- include only primary key
        self.proj('attr1', 'attr2')  -- include primary key and attributes attr1 and attr2
        self.proj(..., '-attr1', '-attr2')  -- include all attributes except attr1 and attr2
        self.proj(name1='attr1') -- include primary key and 'attr1' renamed as name1
        self.proj('attr1', dup='(attr1)') -- include primary key and attribute attr1 twice, with the duplicate 'dup'
        self.proj(k='abs(attr1)') adds the new attribute k with the value computed as an expression (SQL syntax)
        from other attributes available before the projection.
        Each attribute name can only be used once.
        """
        # new attributes in parentheses are included again with the new name without removing original
        duplication_pattern = re.compile(
            fr'^\s*\(\s*(?!{"|".join(CONSTANT_LITERALS)})(?P<name>[a-zA-Z_]\w*)\s*\)\s*$'
        )
        # attributes without parentheses renamed
        rename_pattern = re.compile(
            fr'^\s*(?!{"|".join(CONSTANT_LITERALS)})(?P<name>[a-zA-Z_]\w*)\s*$'
        )
        replicate_map = {
            k: m.group("name")
            for k, m in (
                (k, duplication_pattern.match(v)) for k, v in named_attributes.items()
            )
            if m
        }
        rename_map = {
            k: m.group("name")
            for k, m in (
                (k, rename_pattern.match(v)) for k, v in named_attributes.items()
            )
            if m
        }
        compute_map = {
            k: v
            for k, v in named_attributes.items()
            if not duplication_pattern.match(v) and not rename_pattern.match(v)
        }
        attributes = set(attributes)
        # include primary key
        attributes.update((k for k in self.primary_key if k not in rename_map.values()))
        # include all secondary attributes with Ellipsis
        if Ellipsis in attributes:
            attributes.discard(Ellipsis)
            attributes.update(
                (
                    a
                    for a in self.heading.secondary_attributes
                    if a not in attributes and a not in rename_map.values()
                )
            )
        try:
            raise DataJointError(
                "%s is not a valid data type for an attribute in .proj"
                % next(a for a in attributes if not isinstance(a, str))
            )
        except StopIteration:
            pass  # normal case
        # remove excluded attributes, specified as `-attr'
        excluded = set(a for a in attributes if a.strip().startswith("-"))
        attributes.difference_update(excluded)
        excluded = set(a.lstrip("-").strip() for a in excluded)
        attributes.difference_update(excluded)
        try:
            raise DataJointError(
                "Cannot exclude primary key attribute %s",
                next(a for a in excluded if a in self.primary_key),
            )
        except StopIteration:
            pass  # all ok
        # check that all attributes exist in heading
        try:
            raise DataJointError(
                "Attribute `%s` not found."
                % next(a for a in attributes if a not in self.heading.names)
            )
        except StopIteration:
            pass  # all ok

        # check that all mentioned names are present in heading
        mentions = attributes.union(replicate_map.values()).union(rename_map.values())
        try:
            raise DataJointError(
                "Attribute '%s' not found."
                % next(a for a in mentions if not self.heading.names)
            )
        except StopIteration:
            pass  # all ok

        # check that newly created attributes do not clash with any other selected attributes
        try:
            raise DataJointError(
                "Attribute `%s` already exists"
                % next(
                    a
                    for a in rename_map
                    if a in attributes.union(compute_map).union(replicate_map)
                )
            )
        except StopIteration:
            pass  # all ok
        try:
            raise DataJointError(
                "Attribute `%s` already exists"
                % next(
                    a
                    for a in compute_map
                    if a in attributes.union(rename_map).union(replicate_map)
                )
            )
        except StopIteration:
            pass  # all ok
        try:
            raise DataJointError(
                "Attribute `%s` already exists"
                % next(
                    a
                    for a in replicate_map
                    if a in attributes.union(rename_map).union(compute_map)
                )
            )
        except StopIteration:
            pass  # all ok

        # need a subquery if the projection remaps any remapped attributes
        used = set(q for v in compute_map.values() for q in extract_column_names(v))
        used.update(rename_map.values())
        used.update(replicate_map.values())
        used.intersection_update(self.heading.names)
        need_subquery = isinstance(self, Union) or any(
            self.heading[name].attribute_expression is not None for name in used
        )
        if not need_subquery and self.restriction:
            # need a subquery if the restriction applies to attributes that have been renamed
            need_subquery = any(
                name in self.restriction_attributes
                for name in self.heading.new_attributes
            )

        result = self.make_subquery() if need_subquery else copy.copy(self)
        result._original_heading = result.original_heading
        result._heading = result.heading.select(
            attributes,
            rename_map=dict(**rename_map, **replicate_map),
            compute_map=compute_map,
        )
        return result

    def aggr(self, group, *attributes, keep_all_rows=False, **named_attributes):
        """
        Aggregation of the type U('attr1','attr2').aggr(group, computation="QueryExpression")
        has the primary key ('attr1','attr2') and performs aggregation computations for all matching elements of `group`.
        :param group:  The query expression to be aggregated.
        :param keep_all_rows: True=keep all the rows from self. False=keep only rows that match entries in group.
        :param named_attributes: computations of the form new_attribute="sql expression on attributes of group"
        :return: The derived query expression
        """
        if Ellipsis in attributes:
            # expand ellipsis to include only attributes from the left table
            attributes = set(attributes)
            attributes.discard(Ellipsis)
            attributes.update(self.heading.secondary_attributes)
        return Aggregation.create(self, group=group, keep_all_rows=keep_all_rows).proj(
            *attributes, **named_attributes
        )

    aggregate = aggr  # alias for aggr

    # ---------- Fetch operators --------------------
    @property
    def fetch1(self):
        return Fetch1(self)

    @property
    def fetch(self):
        return Fetch(self)

    def head(self, limit=25, **fetch_kwargs):
        """
        shortcut to fetch the first few entries from query expression.
        Equivalent to fetch(order_by="KEY", limit=25)
        :param limit:  number of entries
        :param fetch_kwargs: kwargs for fetch
        :return: query result
        """
        return self.fetch(order_by="KEY", limit=limit, **fetch_kwargs)

    def tail(self, limit=25, **fetch_kwargs):
        """
        shortcut to fetch the last few entries from query expression.
        Equivalent to fetch(order_by="KEY DESC", limit=25)[::-1]
        :param limit:  number of entries
        :param fetch_kwargs: kwargs for fetch
        :return: query result
        """
        return self.fetch(order_by="KEY DESC", limit=limit, **fetch_kwargs)[::-1]

    def __len__(self):
        """:return: number of elements in the result set e.g. ``len(q1)``."""
        return self.connection.query(
            "SELECT {select_} FROM {from_}{where}".format(
                select_=(
                    "count(*)"
                    if any(self._left)
                    else "count(DISTINCT {fields})".format(
                        fields=self.heading.as_sql(
                            self.primary_key, include_aliases=False
                        )
                    )
                ),
                from_=self.from_clause(),
                where=self.where_clause(),
            )
        ).fetchone()[0]

    def __bool__(self):
        """
        :return: True if the result is not empty. Equivalent to len(self) > 0 but often
            faster e.g. ``bool(q1)``.
        """
        return bool(
            self.connection.query(
                "SELECT EXISTS(SELECT 1 FROM {from_}{where})".format(
                    from_=self.from_clause(), where=self.where_clause()
                )
            ).fetchone()[0]
        )

    def __contains__(self, item):
        """
        returns True if the restriction in item matches any entries in self
            e.g. ``restriction in q1``.
        :param item: any restriction
        (item in query_expression) is equivalent to bool(query_expression & item) but may be
        executed more efficiently.
        """
        return bool(self & item)  # May be optimized e.g. using an EXISTS query

    def __iter__(self):
        """
        returns an iterator-compatible QueryExpression object e.g. ``iter(q1)``.

        :param self: iterator-compatible QueryExpression object
        """
        self._iter_only_key = all(v.in_key for v in self.heading.attributes.values())
        self._iter_keys = self.fetch("KEY")
        return self

    def __next__(self):
        """
        returns the next record on an iterator-compatible QueryExpression object
            e.g. ``next(q1)``.

        :param self: A query expression
        :type self: :class:`QueryExpression`
        :rtype: dict
        """
        try:
            key = self._iter_keys.pop(0)
        except AttributeError:
            # self._iter_keys is missing because __iter__ has not been called.
            raise TypeError(
                "A QueryExpression object is not an iterator. "
                "Use iter(obj) to create an iterator."
            )
        except IndexError:
            raise StopIteration
        else:
            if self._iter_only_key:
                return key
            else:
                try:
                    return (self & key).fetch1()
                except DataJointError:
                    # The data may have been deleted since the moment the keys were fetched
                    # -- move on to next entry.
                    return next(self)

    def cursor(self, offset=0, limit=None, order_by=None, as_dict=False):
        """
        See expression.fetch() for input description.
        :return: query cursor
        """
        if offset and limit is None:
            raise DataJointError("limit is required when offset is set")
        sql = self.make_sql()
        if order_by is not None:
            sql += " ORDER BY " + ", ".join(order_by)
        if limit is not None:
            sql += " LIMIT %d" % limit + (" OFFSET %d" % offset if offset else "")
        logger.debug(sql)
        return self.connection.query(sql, as_dict=as_dict)

    def __repr__(self):
        """
        returns the string representation of a QueryExpression object e.g. ``str(q1)``.

        :param self: A query expression
        :type self: :class:`QueryExpression`
        :rtype: str
        """
        return (
            super().__repr__()
            if config["loglevel"].lower() == "debug"
            else self.preview()
        )

    def preview(self, limit=None, width=None):
        """:return: a string of preview of the contents of the query."""
        return preview(self, limit, width)

    def _repr_html_(self):
        """:return: HTML to display table in Jupyter notebook."""
        return repr_html(self)


class Aggregation(QueryExpression):
    """
    Aggregation.create(arg, group, comp1='calc1', ..., compn='calcn')  yields an entity set
    with primary key from arg.
    The computed arguments comp1, ..., compn use aggregation calculations on the attributes of
    group or simple projections and calculations on the attributes of arg.
    Aggregation is used QueryExpression.aggr and U.aggr.
    Aggregation is a private class in DataJoint, not exposed to users.
    """

    _left_restrict = None  # the pre-GROUP BY conditions for the WHERE clause
    _subquery_alias_count = count()

    @classmethod
    def create(cls, arg, group, keep_all_rows=False):
        if inspect.isclass(group) and issubclass(group, QueryExpression):
            group = group()  # instantiate if a class
        assert isinstance(group, QueryExpression)
        if keep_all_rows and len(group.support) > 1 or group.heading.new_attributes:
            group = group.make_subquery()  # subquery if left joining a join
        join = arg.join(group, left=keep_all_rows)  # reuse the join logic
        result = cls()
        result._connection = join.connection
        result._heading = join.heading.set_primary_key(
            arg.primary_key
        )  # use left operand's primary key
        result._support = join.support
        result._left = join._left
        result._left_restrict = join.restriction  # WHERE clause applied before GROUP BY
        result._grouping_attributes = result.primary_key

        return result

    def where_clause(self):
        return (
            ""
            if not self._left_restrict
            else " WHERE (%s)" % ")AND(".join(str(s) for s in self._left_restrict)
        )

    def make_sql(self, fields=None):
        fields = self.heading.as_sql(fields or self.heading.names)
        assert self._grouping_attributes or not self.restriction
        distinct = set(self.heading.names) == set(self.primary_key)
        return "SELECT {distinct}{fields} FROM {from_}{where}{group_by}".format(
            distinct="DISTINCT " if distinct else "",
            fields=fields,
            from_=self.from_clause(),
            where=self.where_clause(),
            group_by=""
            if not self.primary_key
            else (
                " GROUP BY `%s`" % "`,`".join(self._grouping_attributes)
                + (
                    ""
                    if not self.restriction
                    else " HAVING (%s)" % ")AND(".join(self.restriction)
                )
            ),
        )

    def __len__(self):
        return self.connection.query(
            "SELECT count(1) FROM ({subquery}) `${alias:x}`".format(
                subquery=self.make_sql(), alias=next(self._subquery_alias_count)
            )
        ).fetchone()[0]

    def __bool__(self):
        return bool(
            self.connection.query("SELECT EXISTS({sql})".format(sql=self.make_sql()))
        )


class Union(QueryExpression):
    """
    Union is the private DataJoint class that implements the union operator.
    """

    __count = count()

    @classmethod
    def create(cls, arg1, arg2):
        if inspect.isclass(arg2) and issubclass(arg2, QueryExpression):
            arg2 = arg2()  # instantiate if a class
        if not isinstance(arg2, QueryExpression):
            raise DataJointError(
                "A QueryExpression can only be unioned with another QueryExpression"
            )
        if arg1.connection != arg2.connection:
            raise DataJointError(
                "Cannot operate on QueryExpressions originating from different connections."
            )
        if set(arg1.primary_key) != set(arg2.primary_key):
            raise DataJointError(
                "The operands of a union must share the same primary key."
            )
        if set(arg1.heading.secondary_attributes) & set(
            arg2.heading.secondary_attributes
        ):
            raise DataJointError(
                "The operands of a union must not share any secondary attributes."
            )
        result = cls()
        result._connection = arg1.connection
        result._heading = arg1.heading.join(arg2.heading)
        result._support = [arg1, arg2]
        return result

    def make_sql(self):
        arg1, arg2 = self._support
        if (
            not arg1.heading.secondary_attributes
            and not arg2.heading.secondary_attributes
        ):
            # no secondary attributes: use UNION DISTINCT
            fields = arg1.primary_key
            return "SELECT * FROM (({sql1}) UNION ({sql2})) as `_u{alias}`".format(
                sql1=arg1.make_sql()
                if isinstance(arg1, Union)
                else arg1.make_sql(fields),
                sql2=arg2.make_sql()
                if isinstance(arg2, Union)
                else arg2.make_sql(fields),
                alias=next(self.__count),
            )
        # with secondary attributes, use union of left join with antijoin
        fields = self.heading.names
        sql1 = arg1.join(arg2, left=True).make_sql(fields)
        sql2 = (
            (arg2 - arg1)
            .proj(..., **{k: "NULL" for k in arg1.heading.secondary_attributes})
            .make_sql(fields)
        )
        return "({sql1})  UNION ({sql2})".format(sql1=sql1, sql2=sql2)

    def from_clause(self):
        """The union does not use a FROM clause"""
        assert False

    def where_clause(self):
        """The union does not use a WHERE clause"""
        assert False

    def __len__(self):
        return self.connection.query(
            "SELECT count(1) FROM ({subquery}) `${alias:x}`".format(
                subquery=self.make_sql(),
                alias=next(QueryExpression._subquery_alias_count),
            )
        ).fetchone()[0]

    def __bool__(self):
        return bool(
            self.connection.query("SELECT EXISTS({sql})".format(sql=self.make_sql()))
        )


class U:
    """
    dj.U objects are the universal sets representing all possible values of their attributes.
    dj.U objects cannot be queried on their own but are useful for forming some queries.
    dj.U('attr1', ..., 'attrn') represents the universal set with the primary key attributes attr1 ... attrn.
    The universal set is the set of all possible combinations of values of the attributes.
    Without any attributes, dj.U() represents the set with one element that has no attributes.

    Restriction:

    dj.U can be used to enumerate unique combinations of values of attributes from other expressions.

    The following expression yields all unique combinations of contrast and brightness found in the `stimulus` set:

    >>> dj.U('contrast', 'brightness') & stimulus

    Aggregation:

    In aggregation, dj.U is used for summary calculation over an entire set:

    The following expression yields one element with one attribute `s` containing the total number of elements in
    query expression `expr`:

    >>> dj.U().aggr(expr, n='count(*)')

    The following expressions both yield one element containing the number `n` of distinct values of attribute `attr` in
    query expressio `expr`.

    >>> dj.U().aggr(expr, n='count(distinct attr)')
    >>> dj.U().aggr(dj.U('attr').aggr(expr), 'n=count(*)')

    The following expression yields one element and one attribute `s` containing the sum of values of attribute `attr`
    over entire result set of expression `expr`:

    >>> dj.U().aggr(expr, s='sum(attr)')

    The following expression yields the set of all unique combinations of attributes `attr1`, `attr2` and the number of
    their occurrences in the result set of query expression `expr`.

    >>> dj.U(attr1,attr2).aggr(expr, n='count(*)')

    Joins:

    If expression `expr` has attributes 'attr1' and 'attr2', then expr * dj.U('attr1','attr2') yields the same result
    as `expr` but `attr1` and `attr2` are promoted to the the primary key.  This is useful for producing a join on
    non-primary key attributes.
    For example, if `attr` is in both expr1 and expr2 but not in their primary keys, then expr1 * expr2 will throw
    an error because in most cases, it does not make sense to join on non-primary key attributes and users must first
    rename `attr` in one of the operands.  The expression dj.U('attr') * rel1 * rel2 overrides this constraint.
    """

    def __init__(self, *primary_key):
        self._primary_key = primary_key

    @property
    def primary_key(self):
        return self._primary_key

    def __and__(self, other):
        if inspect.isclass(other) and issubclass(other, QueryExpression):
            other = other()  # instantiate if a class
        if not isinstance(other, QueryExpression):
            raise DataJointError("Set U can only be restricted with a QueryExpression.")
        result = copy.copy(other)
        result._distinct = True
        result._heading = result.heading.set_primary_key(self.primary_key)
        result = result.proj()
        return result

    def join(self, other, left=False):
        """
        Joining U with a query expression has the effect of promoting the attributes of U to
        the primary key of the other query expression.

        :param other: the other query expression to join with.
        :param left: ignored. dj.U always acts as if left=False
        :return: a copy of the other query expression with the primary key extended.
        """
        if inspect.isclass(other) and issubclass(other, QueryExpression):
            other = other()  # instantiate if a class
        if not isinstance(other, QueryExpression):
            raise DataJointError("Set U can only be joined with a QueryExpression.")
        try:
            raise DataJointError(
                "Attribute `%s` not found"
                % next(k for k in self.primary_key if k not in other.heading.names)
            )
        except StopIteration:
            pass  # all ok
        result = copy.copy(other)
        result._heading = result.heading.set_primary_key(
            other.primary_key
            + [k for k in self.primary_key if k not in other.primary_key]
        )
        return result

    def __mul__(self, other):
        """shorthand for join"""
        return self.join(other)

    def aggr(self, group, **named_attributes):
        """
        Aggregation of the type U('attr1','attr2').aggr(group, computation="QueryExpression")
        has the primary key ('attr1','attr2') and performs aggregation computations for all matching elements of `group`.
        :param group:  The query expression to be aggregated.
        :param named_attributes: computations of the form new_attribute="sql expression on attributes of group"
        :return: The derived query expression
        """
        if named_attributes.get("keep_all_rows", False):
            raise DataJointError(
                "Cannot set keep_all_rows=True when aggregating on a universal set."
            )
        return Aggregation.create(self, group=group, keep_all_rows=False).proj(
            **named_attributes
        )

    aggregate = aggr  # alias for aggr
