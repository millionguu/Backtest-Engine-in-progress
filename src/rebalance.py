class Rebalance:
    def __init__(
        self, period, portfolio, factor, blacklist, disable_rebalance=False
    ) -> None:
        self.period = period
        self.portfolio = portfolio
        self.factor = factor
        self.blacklist = blacklist
        self.disable_rebalance = disable_rebalance

    def run(self, cur_date):
        if self.disable_rebalance:
            return
        position = self.factor.get_position(cur_date)

        residual = 0
        valid_count = 0
        new_position = []
        for s, w in position:
            if s not in self.blacklist:
                valid_count += 1
                if valid_count < len(position):
                    new_position.append((s, w))
                else:
                    new_position.append((s, w - 0.01))  # rounding error
            else:
                new_position.append((s, 0))
                residual += w

        if residual > 0:
            residual -= 0.01  # rounding error
            new_position = [
                (s, w + round(residual / valid_count, 3)) if w != 0 else (s, 0)
                for s, w in new_position
            ]

        position_change = []

        new_securities = [s for s, _ in new_position]
        for security in self.portfolio.security_book.keys():
            condition = self.portfolio.security_book[security]["date"] == cur_date
            original_weight = self.portfolio.security_book[security][condition][
                "weight"
            ].iloc[0]
            if security not in new_securities and original_weight > 0:
                position_change.append((security, -original_weight))

        for security, weight in new_position:
            condition = self.portfolio.security_book[security]["date"] == cur_date
            original_weight = self.portfolio.security_book[security][condition][
                "weight"
            ].iloc[0]
            position_change.append((security, weight - original_weight))

        # sold first and then buy
        position_change.sort(key=lambda p: p[1])
        print(f"rebalance on {cur_date}: {position_change}")

        for security, weight_change in position_change:
            if weight_change < 0:
                self.portfolio.reduce_security_weight(
                    security, abs(weight_change), cur_date
                )
            elif weight_change > 0:
                self.portfolio.add_security_weight(security, weight_change, cur_date)
            else:
                pass
