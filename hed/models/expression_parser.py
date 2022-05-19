import re


# Syntax:
# and = "and" or ","
# or = "or"
# logical operation grouping = "(" and ")"
# inexact grouping = "[" and "]"
# same group grouping = "[[" and "]]"
# not = "~"

class Token:
    And = 0
    Tag = 1
    ExactGroup = 2
    ExactGroupEnd = 3
    ContainingGroup = 8
    ContainingGroupEnd = 9
    Or = 4
    LogicalGroup = 5
    LogicalGroupEnd = 6
    LogicalNegation = 7

    def __init__(self, text):
        tokens = {
            ",": Token.And,
            "and": Token.And,
            "or": Token.Or,
            "[[": Token.ExactGroup,
            "]]": Token.ExactGroupEnd,
            "[": Token.ContainingGroup,
            "]": Token.ContainingGroupEnd,
            "(": Token.LogicalGroup,
            ")": Token.LogicalGroupEnd,
            "~": Token.LogicalNegation,
        }
        self.kind = tokens.get(text, Token.Tag)
        self.text = text

    def __str__(self):
        return self.text

    def __eq__(self, other):
        if self.kind == other:
            return True
        return False


class Expression:
    def __init__(self, token, left=None, right=None):
        self.left = left
        self.right = right
        self.token = token

    def __str__(self):
        output_str = ""
        if self.left:
            output_str += str(self.left)
        output_str += " " + str(self.token)
        if self.right:
            output_str += str(self.right)
        return output_str

    def handle_expr(self, hed_group, exact=False):
        # groups_found = hed_group.find_exact_tags([self.token.text], recursive=True)
        groups_found = hed_group.find_tags_with_term(self.token.text, recursive=True, include_groups=1)
        if exact:
            all_found_groups = groups_found
        else:
            all_found_groups = []
            for group in groups_found:
                while group:
                    all_found_groups.append(group)
                    group = group._parent
        return all_found_groups


class ExpressionAnd(Expression):
    def handle_expr(self, hed_group, exact=False):
        groups1 = self.left.handle_expr(hed_group, exact=exact)
        if not groups1:
            return groups1
        groups2 = self.right.handle_expr(hed_group, exact=exact)
        return [group for group in groups1 if group in groups2]


class ExpressionOr(Expression):
    def handle_expr(self, hed_group, exact=False):
        groups1 = self.left.handle_expr(hed_group, exact=exact)
        # Don't early out as we need to gather all groups incase tags appear more than once etc
        groups2 = self.right.handle_expr(hed_group, exact=exact)
        return groups1 + groups2


class ExpressionNegation(Expression):
    def handle_expr(self, hed_group, exact=False):
        groups = self.right.handle_expr(hed_group, exact=exact)

        # todo: This probably needs fixing
        negated_groups = [group for group in hed_group.get_all_groups() if group not in groups]
        # Possible alternate if only groups with tags are valid results(is this a real case?)
        #negated_groups = [group for group in hed_group.get_all_groups() if group not in groups if group.tags()]

        return negated_groups


class ExpressionLogicalGroup(Expression):
    def handle_expr(self, hed_group, exact=False):
        return self.right.handle_expr(hed_group, exact=exact)


class ExpressionExactGroup(Expression):
    def handle_expr(self, hed_group, exact=False):
        result = self.right.handle_expr(hed_group, exact=True)
        found_groups = result
        if result:
            found_parent_groups = list()
            for group in found_groups:
                if not group.is_group:
                    continue
                if group._parent:
                    found_parent_groups.append(group._parent)

            if found_parent_groups:
                return found_parent_groups

        return []


class ExpressionContainingGroup(Expression):
    def handle_expr(self, hed_group, exact=False):
        result = self.right.handle_expr(hed_group)
        found_groups = result
        found_parent_groups = list()
        if result:
            for group in found_groups:
                if not group.is_group:
                    continue
                if group._parent:
                    found_parent_groups.append(group._parent)


        if found_parent_groups:
            return found_parent_groups
        return []


class TagExpressionParser:
    """Parse a search expression into a form than can be used to search a hed string."""
    def __init__(self, expression_string):
        self.tokens = []
        self.at_token = -1
        self.tree = self._parse(expression_string.lower())
        self._org_string = expression_string

    def _get_next_token(self):
        self.at_token += 1
        if self.at_token >= len(self.tokens):
            raise ValueError("Parse error in get next token")
        return self.tokens[self.at_token]

    def _next_token_is(self, kinds):
        if self.at_token + 1 >= len(self.tokens):
            return None
        if self.tokens[self.at_token + 1].kind in kinds:
            return self._get_next_token()
        return None

    def _handle_and_op(self):
        expr = self._handle_negation()
        next_token = self._next_token_is([Token.And, Token.Or])
        while next_token:
            right = self._handle_negation()
            if next_token.kind == Token.And:
                expr = ExpressionAnd(next_token, expr, right)
            else:
                expr = ExpressionOr(next_token, expr, right)
            next_token = self._next_token_is([Token.And, Token.Or])

        return expr

    def _handle_negation(self):
        next_token = self._next_token_is([Token.LogicalNegation])
        if next_token == Token.LogicalNegation:
            interior = self._handle_grouping_op()
            expr = ExpressionNegation(next_token, right=interior)
            return expr
        else:
            return self._handle_grouping_op()

    def _handle_grouping_op(self):
        next_token = self._next_token_is([Token.ExactGroup, Token.LogicalGroup, Token.ContainingGroup])
        if next_token == Token.ExactGroup:
            interior = self._handle_and_op()
            expr = ExpressionExactGroup(next_token, right=interior)
            next_token = self._next_token_is([Token.ExactGroupEnd])
            if next_token != Token.ExactGroupEnd:
                raise ValueError("Parse error: Missing closing square brackets")
        elif next_token == Token.LogicalGroup:
            interior = self._handle_and_op()
            expr = ExpressionLogicalGroup(next_token, right=interior)
            next_token = self._next_token_is([Token.LogicalGroupEnd])
            if next_token != Token.LogicalGroupEnd:
                raise ValueError("Parse error: Missing closing paren")
        elif next_token == Token.ContainingGroup:
            interior = self._handle_and_op()
            expr = ExpressionContainingGroup(next_token, right=interior)
            next_token = self._next_token_is([Token.ContainingGroupEnd])
            if next_token != Token.ContainingGroupEnd:
                raise ValueError("Parse error: Missing closing square bracket")
        else:
            next_token = self._get_next_token()
            if next_token:
                expr = Expression(next_token)

        return expr

    def _parse(self, expression_string):
        self.tokens = self._tokenize(expression_string)

        expr = self._handle_and_op()

        if self.at_token + 1 != len(self.tokens):
            raise ValueError("Parse error in search string")

        return expr

    def _tokenize(self, expression_string):
        grouping_re = r"\[\[|\[|,|\]\]|\]"
        paren_re = r"\)|\(|~"
        word_re = r"\band\b|\bor\b|[_\-a-zA-Z0-9/.^#]+"
        re_string = fr"({grouping_re}|{paren_re}|{word_re})"
        token_re = re.compile(re_string)

        tokens = token_re.findall(expression_string)
        tokens = [Token(token) for token in tokens]

        return tokens

    def search_hed_string(self, hed_string_obj):
        current_node = self.tree

        result = current_node.handle_expr(hed_string_obj)
        return result


if __name__ == '__main__':
    from hed.models.hed_string import HedStringFrozen
    from hed.schema.hed_schema_io import load_schema_version
    hed_schema = load_schema_version(xml_version='8.0.0')
    my_string = "Sensory-event,(Intended-effect,Cue),((Def-expand/Circle-only," + \
                "(Visual-presentation,(Foreground-view,((White,Circle)," + \
                "(Center-of,Computer-screen))),(Background-view,Black))),Onset)," + \
                "((Def-expand/Face-image,(Visual-presentation,(Foreground-view,((Image,Face,Hair)," + \
                "Color/Grayscale),((White,Cross),(Center-of,Computer-screen)))," + \
                "(Background-view,Black))),Offset),((Def-expand/Blink-inhibition-task," + \
                "(Task,Experiment-participant,Inhibit-blinks)),Offset)," + \
                "((Def-expand/Fixation-task,(Task,Experiment-participant,(Fixate,Cross)" + \
                ")),Offset),Experimental-trial/1,(Image,Pathname/circle.bmp)"
    hed_string = HedStringFrozen(my_string, hed_schema=hed_schema)
    my_string1 = "sensory-event,(intended-effect,cue)"
    hed_string1 = HedStringFrozen(my_string1, hed_schema=hed_schema)
    expression1 = TagExpressionParser("sensory-event")
    print(expression1.search_hed_string(hed_string1), True)
    expression2 = TagExpressionParser("sensory-event, cue")
    print(expression2.search_hed_string(hed_string1), True)
    expression3 = TagExpressionParser("onset")
    print(expression3.search_hed_string(hed_string), True)
    expression = TagExpressionParser("[[a,b]]")
    hed_string = HedStringFrozen("A, C, D, B")
    x = expression.search_hed_string(hed_string)
    print(expression.search_hed_string(hed_string), False)  # False
    hed_string = HedStringFrozen("(A, B, C, D)")
    y = expression.search_hed_string(hed_string)
    print(expression.search_hed_string(hed_string), True)  # True
    hed_string = HedStringFrozen("(A, C, D, B)")
    print(expression.search_hed_string(hed_string), True)  # True

    # Doesn't handle and/or precedence well(right to left and equal precedent currently)
    expression = TagExpressionParser("a and b or c")
    hed_string = HedStringFrozen("B")
    print(expression.search_hed_string(hed_string), False)  # False
    hed_string = HedStringFrozen("A, B")
    print(expression.search_hed_string(hed_string), True)  # True
    hed_string = HedStringFrozen("A, C")
    print(expression.search_hed_string(hed_string), True)  # True
    hed_string = HedStringFrozen("C")
    print(expression.search_hed_string(hed_string), True)  # TRue

    expression = TagExpressionParser("a or b and c")
    hed_string = HedStringFrozen("B")
    print(expression.search_hed_string(hed_string), False)  # False
    hed_string = HedStringFrozen("A, B")
    print(expression.search_hed_string(hed_string), False)  # False
    hed_string = HedStringFrozen("A, C")
    print(expression.search_hed_string(hed_string), True)  # True
    hed_string = HedStringFrozen("C")
    print(expression.search_hed_string(hed_string), False)  # false
    hed_string = HedStringFrozen("A")
    print(expression.search_hed_string(hed_string), False)  # false

    expression = TagExpressionParser("a and (b or c)")
    hed_string = HedStringFrozen("A, C")
    print(expression.search_hed_string(hed_string), True)  # True

    expression = TagExpressionParser("[[a,b]] or [[c, d]]")
    hed_string = HedStringFrozen("(c, d)")
    print(expression.search_hed_string(hed_string), True)  # True
    hed_string = HedStringFrozen("(b, d)")
    print(expression.search_hed_string(hed_string), False)  # False
    hed_string = HedStringFrozen("(b, a)")
    print(expression.search_hed_string(hed_string), True)  # True

    expression = TagExpressionParser("[[a,b]] and [[c, d]]")
    hed_string = HedStringFrozen("(c, d, a, b)")
    print(expression.search_hed_string(hed_string), True)  # True
    hed_string = HedStringFrozen("(c, d, a)")
    print(expression.search_hed_string(hed_string), False)  # False
    hed_string = HedStringFrozen("(c, d), (a, b)")
    print(expression.search_hed_string(hed_string), True)  # True
    breakHere = 3

    expression = TagExpressionParser("[[a,b]] and d")
    hed_string = HedStringFrozen("(a, b), d")
    print(expression.search_hed_string(hed_string), True)  # True

    expression = TagExpressionParser("[[a,b]] and d")
    hed_string = HedStringFrozen("(a, b), c")
    print(expression.search_hed_string(hed_string), False)  # False

    expression = TagExpressionParser("[[a,b]] or d")
    hed_string = HedStringFrozen("d")
    print(expression.search_hed_string(hed_string), True)  # True

    hed_string = HedStringFrozen("a, b")
    print(expression.search_hed_string(hed_string), False)  # False

    expression = TagExpressionParser("[[a,b,[[c,d]]]]")
    # # Should this be valid?  It's iffy.
    hed_string = HedStringFrozen("(a, b, c, d)")
    print(expression.search_hed_string(hed_string), False)  # False

    hed_string = HedStringFrozen("(a, b, (c, d))")
    y = expression.search_hed_string(hed_string)
    print(expression.search_hed_string(hed_string), True)  # True

    expression = TagExpressionParser("[[a,b,[[c,d, [[e, f]]]]]]")
    hed_string = HedStringFrozen("(a, b, ((e, f), c, d))")
    print(expression.search_hed_string(hed_string), True)  # True

    expression = TagExpressionParser("[[a,b,[[c,d, [[e, f]]]]]]")
    hed_string = HedStringFrozen("(a, b, (e, f, c, d))")
    print(expression.search_hed_string(hed_string), False)  # False

