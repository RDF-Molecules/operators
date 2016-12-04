class MergeOp():

    # def __init__(self):
    #     # self.left = rtl1
    #     # self.right = rtl2

    def execute(self, (rtl1, rtl2)):
        self.left = rtl1['tail']
        self.right = rtl2['tail']
        self.merged = {}

        # retain URI of the first RTL, doesn't matter much
        self.merged['head'] = rtl1['head']
        self.merged['tail'] = []

        # check tails of RTLs and find relevant merge cases
        for pv_left in self.left:
            for pv_right in self.right:
                # if p1 = p2 and v1 = v2 then just pass the pair into the merged entity
                if (pv_left['prop']==pv_right['prop']) and (pv_left['value']==pv_right['value']):
                    self.merged['tail'].append(pv_left)
                # or if p1 = p2 but v1 != v2
                elif pv_left['prop']==pv_right['prop']:
                    # prop case
                    # check if prop is functional
                    pass
                # or if p1 != p2 but v1 = v2
                elif pv_left['value']==pv_right['value']:
                    # value case
                    pass
            # no relevant pair for pv_left , consider as a standalone pv pair
            self.merged['tail'].append(pv_left)

    def prop_case(self):
        pass

    def value_case(self):
        pass



